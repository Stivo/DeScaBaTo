package ch.descabato.core.actors

import ch.descabato.core.model.Block
import ch.descabato.core_old.BackupFolderConfiguration
import ch.descabato.utils.Implicits._
import ch.descabato.utils.{CompressedStream, Utils}
import ch.descabato.{CompressionMode, TimingUtil}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

object Compressors {
  def apply(backupFolderConfiguration: BackupFolderConfiguration) = {
    backupFolderConfiguration.compressor match {
      case CompressionMode.smart =>
        new Smart3Compressor()
      case x if x.isCompressionAlgorithm =>
        new SimpleCompressor(backupFolderConfiguration.compressor)
      case x =>
        throw new IllegalArgumentException(s"Unknown compression algorithm $x")
    }
  }
}

trait Compressor {
  def compressBlock(block: Block): Future[Block]
  def report(): Unit
}

class SimpleCompressor(val mode: CompressionMode) extends Compressor {
  override def compressBlock(block: Block): Future[Block] = {
    Future {
      block.compressed = CompressedStream.compress(block.content, mode)
      block
    }
  }

  override def report(): Unit = {}
}

sealed trait CompressionDeciderState
// we still send every block to all compression deciders to see which is best
case object Sampling extends CompressionDeciderState
// we still send every block to all compression deciders to see which is best
case object SamplingForLzma extends CompressionDeciderState
// we send everything to the winner now
case class Done(winner: CompressionMode) extends CompressionDeciderState

class Smart3Compressor extends Compressor with Utils {

  val sampleBytes: Long = 4 * 1024 * 1024

  def extensionFor(block: Block): Extension = {
    val name = block.blockId.fd.pathParts.last
    val extension = if (name.contains('.')) {
      name.drop(name.lastIndexOf('.') + 1)
    } else {
      name
    }
    if (!extensions.safeContains(extension)) {
      extensions += extension -> new Extension(extension)
    }
    extensions(extension)
  }

  val algosFirstRound: Seq[CompressionMode] = Array(CompressionMode.snappy, CompressionMode.lz4hc, CompressionMode.gzip)

  class Extension(val ext: String) {

    var algos: Seq[CompressionMode] = algosFirstRound

    var bytesReceived: Long = 0

    class SampleData {
      private var _compressedBytes = 0
      private var _totalTime = 0L

      def blockArrived(block: Block, time: Long): Unit = {
        _compressedBytes += block.compressed.length
        _totalTime += time
      }

      def totalTime: Long = _totalTime

      // lower is better
      def ratio: Double = {
        if (_compressedBytes == 0) {
          1
        } else {
          _compressedBytes * 1.0 / bytesReceived
        }
      }

      // more than a 3 percent gap
      def isBetterThan(winnerData: SampleData): Boolean = {
        ratio + 0.03 < winnerData.ratio
      }

    }

    private var sampleData: Map[CompressionMode, SampleData] = algos.map(algo => (algo, new SampleData())).toMap

    private[Smart3Compressor] var status: CompressionDeciderState = Sampling

    def compressBlock(block: Block): Future[Block] = {
      status match {
        case Sampling | SamplingForLzma =>
          val blocks = compressWithAllAlgosAndWait(block)
          bytesReceived += block.content.length
          updateSampleData(blocks)
          chooseWinnerIfReady()
          Future.successful(blocks.map(_._1).minBy(_.compressed.length))
        case Done(winner) =>
          compressBlockForReturn(block, winner)
      }
    }

    private def chooseWinnerIfReady() = {
      if (readyToChooseWinner()) {
        updateStatus()
      }
    }

    def freeSamplingData(): Unit = {
      sampleData = Map.empty
    }

    def updateStatus(): Unit = {
      val winner = chooseWinner()
      val newStatus = status match {
          // try again with lzma if it is:
          // - at all compressible
          // - LZMA still has a chance to be 3% better
          // - We havent already sampled with LZMA in the mix
        case Sampling if sampleData(winner).ratio < 0.9 && sampleData(winner).ratio > 0.03 && status == Sampling =>
          bytesReceived = 0
          algos = Seq(winner, CompressionMode.lzma)
          sampleData = algos.map(x => (x, new SampleData())).toMap
          logger.info(s"Doing shootout between $algos for $ext")
          SamplingForLzma
        case Sampling | SamplingForLzma =>
          freeSamplingData()
          Done(winner)
      }
      status = newStatus
    }

    def chooseWinner(): CompressionMode = {
      val candidates = sampleData.toList.sortBy(_._2.totalTime)
      var (winner, winnerData) = candidates.head
      for ((mode, data) <- candidates.tail) {
        if (data.isBetterThan(winnerData)) {
          logger.info(s"Mode $mode (ratio ${data.ratio}) chosen over $winner (ratio ${winnerData.ratio})")
          winner = mode
          winnerData = data
        } else {
          logger.info(s"Mode $mode (ratio ${data.ratio}) not chosen over $winner (ratio ${winnerData.ratio})")
        }
      }
      logger.info(s"Sampling for $ext is done and winner is $winner with ${winnerData.ratio}")
      winner
    }

    private def updateSampleData(blocks: Seq[(Block, CompressionMode, Long)]) = {
      for ((b, algo, time) <- blocks) {
        sampleData(algo).blockArrived(b, time)
      }
    }

    private def readyToChooseWinner(): Boolean = {
      bytesReceived >= sampleBytes
    }

    private def compressWithAllAlgosAndWait(block: Block): Seq[(Block, CompressionMode, Long)] = {
      val futures = algos.map { algo =>
        compressBlockAsync(block.copy(), algo)
      }
      Await.result(Future.sequence(futures), 10.minutes)
    }
  }

  private var extensions: Map[String, Extension] = Map.empty

  override def compressBlock(block: Block): Future[Block] = {
    val mode = if (block.content.length < 256) {
      Some(CompressionMode.none)
    } else if (block.content.length < 2048) {
      Some(CompressionMode.deflate)
    } else {
      None
    }
    mode match {
      case Some(m) =>
        compressBlockForReturn(block, m)
      case None =>
        extensionFor(block).compressBlock(block)
    }
  }

  private def compressBlockAsync(block: Block, mode: CompressionMode): Future[(Block, CompressionMode, Long)] = {
    Future {
      val startAt = TimingUtil.getCpuTime
      val compressed = CompressedStream.compress(block.content, mode)
      block.compressed = compressed
      val duration = TimingUtil.getCpuTime - startAt
      (block, mode, duration)
    }
  }

  private def compressBlockForReturn(block: Block, mode: CompressionMode) = {
    compressBlockAsync(block, mode).map(_._1)
  }

  override def report(): Unit = {
    val modes = extensions.values.map(_.status).collect { case Done(winner) => (winner, 1) }.groupBy(_._1).mapValues(_.size)
    for ((algo, times) <- modes.toList.sortBy(_._1.getEstimatedTime)) {
      logger.info(s"Chose $algo $times")
    }
  }
}
