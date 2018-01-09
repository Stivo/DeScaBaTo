package ch.descabato.core

import ch.descabato.utils.Implicits._
import ch.descabato.utils.{CompressedStream, Utils}
import ch.descabato.{CompressionMode, TimingUtil}

import scala.collection.mutable
import scala.language.implicitConversions

class SimpleCompressionDecider(val overrideMode: Option[CompressionMode]) extends CompressionDecider with UniversePart {
  def this() {
    this(None)
  }

  lazy val mode: CompressionMode = overrideMode.getOrElse(universe.config.compressor)

  def blockCompressed(block: Block, nanoTime: Long) {
    // nobody should be calling this
    ???
  }

  def compressBlock(block: Block): Unit = {
    universe.scheduleTask { () =>
      block.mode = mode
      val compressed = CompressedStream.compress(block.content, block.mode)
      block.compressed = compressed
      universe.blockHandler().writeCompressedBlock(block)
    }
  }

  def report() {

  }
}

object StatisticHelper {

  var defaultLimit = 101

  implicit def longbuf2sortedbuf(buf: mutable.Buffer[Long]): SortedBuffer[Long] = {
    new SortedBuffer[Long](buf)
  }

  implicit def floatbuf2sortedbuf(buf: mutable.Buffer[Float]): SortedBuffer[Float] = {
    new SortedBuffer[Float](buf)
  }

  implicit class SortedBuffer[T: Numeric](buf: mutable.Buffer[T])(implicit num: Numeric[T]) {
    def median(default: T): T = {
      if (buf.isEmpty) {
        default
      } else {
        val middle = buf.size / 2
        buf(middle)
      }
    }

    def insertSorted(x: T) {
      if (buf.lengthCompare(2 * defaultLimit) > 0)
        return
      buf += x
      if (buf.lengthCompare(1) == 0)
        return
      // insertion sort in place
      var i = buf.size - 2
      while (i >= 0) {
        if (num.lt(buf(i + 1), buf(i))) {
          swap(i + 1, i)
          i -= 1
        } else {
          i = -1
        }
      }
    }

    def swap(i1: Int, i2: Int) {
      val bak = buf(i1)
      buf(i1) = buf(i2)
      buf(i2) = bak
    }
  }

}


sealed trait CompressionDeciderState
// we still send every block to all 4 compression deciders to see which is best
case object Sampling extends CompressionDeciderState
// we just send everything to gzip while we wait for the results (good tradeoff)
case object Waiting extends CompressionDeciderState
// we send everything to the winner now
case class Done(val winner: CompressionMode) extends CompressionDeciderState

class Smart2CompressionDecider extends CompressionDecider with UniversePart with Utils {

  val sampleBytes: Long = 2 * 1024 * 1024

  def extensionFor(block: Block): Extension = {
    val name = block.id.file.name
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

  val algos: Seq[CompressionMode] = Array(CompressionMode.none, CompressionMode.snappy, CompressionMode.gzip, CompressionMode.lzma)

  class Extension(val ext: String) {

    var samplesSent = 0
    var samplesReceived = 0

    class SampleData {

      private var _bytesReceived = 0
      private var _compressedBytes = 0
      private var _totalTime = 0L

      def blockArrived(block: Block, time: Long): Unit = {
        _bytesReceived += block.content.length
        _compressedBytes += block.compressed.length
        _totalTime += time
      }

      def totalTime: Long = _totalTime

      // lower is better
      def ratio: Double = {
        if (_compressedBytes == 0) {
          1
        } else {
          _compressedBytes * 1.0 / _bytesReceived
        }
      }

      // more than a 3 percent gap
      def isBetterThan(winnerData: SampleData): Boolean = {
        ratio + 0.03 < winnerData.ratio
      }

    }

    private var sampledBlocks: Map[BlockId, Seq[Block]] = Map.empty
    private var sampleData: Map[CompressionMode, SampleData] = algos.map(algo => (algo, new SampleData())).toMap

    private var _status: CompressionDeciderState = Sampling

    def status: CompressionDeciderState = _status

    def copyBlock(bIn: Block, mode: CompressionMode) = {
      val b = new Block(bIn.id, bIn.content)
      b.hash = bIn.hash
      b.mode = mode
      b
    }

    def compressBlock(block: Block): Unit = {
      _status match {
        case Sampling =>
          sampledBlocks += block.id -> Seq.empty
          for (algo <- algos) {
            compressBlockAsync(copyBlock(block, algo))
          }
          samplesSent += block.content.length
          if (samplesSent > sampleBytes && _status == Sampling) {
            _status = Waiting
          }
        case Waiting =>
          block.mode = CompressionMode.gzip
          compressBlockAsync(block)
        case Done(winner) =>
          block.mode = winner
          compressBlockAsync(block)
      }
    }

    def sampledBlockIsDone(block: Block): Boolean = sampledBlocks(block.id).size == algos.size

    def writeCompressedBlock(block: Block) = {
      universe.blockHandler().writeCompressedBlock(block)
    }

    def writeBlockWithBestCompression(block: Block) = {
      val blocks = sampledBlocks(block.id)
      writeCompressedBlock(blocks.minBy(_.compressed.length))
    }

    def freeSamplingData(): Unit = {
      sampleData = Map.empty
    }

    def chooseWinner(): CompressionMode = {
      val candidates = sampleData.toList.sortBy(_._2.totalTime)
      var (winner, winnerData) = candidates.head
      for ((mode, data) <- candidates.tail) {
        if (data.isBetterThan(winnerData)) {
          winner = mode
          winnerData = data
        }
      }
      logger.info(s"Sampling for $ext is done and winner is $winner with ${winnerData.ratio}")
      winner
    }

    def updateSampleData(block: Block, nanoTime: Long) = {
      sampleData(block.mode).blockArrived(block, nanoTime)
    }

    def blockWasCompressed(block: Block, nanoTime: Long): Unit = {
      if (sampledBlocks.safeContains(block.id)) {
        // update sampled blocks
        sampledBlocks += block.id -> (sampledBlocks(block.id) :+ block)
        updateSampleData(block, nanoTime)
        if (sampledBlockIsDone(block)) {
          writeBlockWithBestCompression(block)
          samplesReceived += block.content.length
          sampledBlocks -= block.id
          if (readyToChooseWinner()) {
            _status = Done(chooseWinner())
            freeSamplingData()
          }
        }
      } else {
        writeCompressedBlock(block)
      }
    }

    private def readyToChooseWinner(): Boolean = {
      samplesReceived == samplesSent && samplesSent >= sampleBytes
    }
  }

  var extensions: Map[String, Extension] = Map.empty

  override def compressBlock(block: Block): Unit = {
    val mode = if (block.uncompressedLength < 256) {
      Some(CompressionMode.none)
    } else if (block.uncompressedLength < 2048) {
      Some(CompressionMode.deflate)
    } else {
      None
    }
    mode match {
      case Some(m) =>
        block.mode = m
        compressBlockAsync(block)
      case None =>
        extensionFor(block).compressBlock(block)
    }
  }

  def compressBlockAsync(block: Block) {
    universe.scheduleTask { () =>
      val startAt = TimingUtil.getCpuTime
      val compressed = CompressedStream.compress(block.content, block.mode)
      block.compressed = compressed
      val duration = TimingUtil.getCpuTime - startAt
      universe.compressionDecider.blockCompressed(block, duration)
    }
  }

  override def blockCompressed(block: Block, nanoTime: Long): Unit = {
    extensionFor(block).blockWasCompressed(block, nanoTime)
  }

  override def report(): Unit = {
    val modes = extensions.values.map(_.status).collect { case Done(winner) => (winner, 1) }.groupBy(_._1).mapValues(_.size)
    for ((algo, times) <- modes.toList.sortBy(_._1.getEstimatedTime)) {
      logger.info(s"Chose $algo $times")
    }
  }
}

class SmartCompressionDecider extends CompressionDecider with UniversePart with Utils {

  import ch.descabato.core.StatisticHelper._

  val samples = 13

  val algos: Seq[CompressionMode] = Array(CompressionMode.none, CompressionMode.snappy, CompressionMode.gzip, CompressionMode.lzma)

  var speeds: Map[CompressionMode, mutable.Buffer[Long]] = algos.map(x => (x, mutable.Buffer[Long](x.getEstimatedTime))).toMap

  var statistics: Map[CompressionMode, Int] = speeds.mapValues(_ => 0)

  /**
    * Collects sampling data for one extension.
    * Once enough samples are in a winner is declared, until then
    * each block is sent to be compressed with each mode and results compared.
    */
  class SamplingData(val ext: String) {
    var ratioSamples: Map[CompressionMode, mutable.Buffer[Float]] = speeds.mapValues(_ => mutable.Buffer[Float]())
    val samplingBlocks: mutable.HashMap[(Int, String), Map[CompressionMode, SamplingBlock]] = mutable.HashMap.empty

    def idForBlock(b: Block): (Int, String) = (b.id.part, b.id.file.path)

    def sampleFile(block: Block): Iterable[Block] = {
      // create new blocks with each algorithm
      val samples = algos.map(x => (x, new SamplingBlock(x, block))).toMap
      samplingBlocks += idForBlock(block) -> samples
      samples.values.map(_.block)
    }

    def samplingFinished: Boolean = ratioSamples.head._2.lengthCompare(samples) > 0

    def blockWasCompressed(b: Block): Option[Block] = {
      val id = idForBlock(b)
      if (!(samplingBlocks safeContains id)) {
        Some(b)
      } else {
        val samplesMap = samplingBlocks(id)
        samplesMap(b.mode).block = b
        samplesMap(b.mode).resultArrived = true
        if (samplesMap.values.forall(_.resultArrived)) {
          samplesMap.foreach { case (mode, sample) =>
            val modeSamples = ratioSamples(mode)
            modeSamples insertSorted sample.ratio
            ratioSamples += mode -> modeSamples
            //            ratioSamples(mode) insertSorted sample.ratio
          }
          samplingBlocks -= idForBlock(b)
          checkIfSamplingDone()
          Some(samplesMap.values.minBy(_.ratio).block)
        } else {
          None
        }
      }
    }

    var winner: CompressionMode = _

    def checkIfSamplingDone() {
      if (samplingFinished) {
        val winnerBackup = winner
        // candidates are all algos
        var candidates: Seq[(CompressionMode, Long)] = speeds.mapValues(_.median(config.blockSize.bytes * 10)).toSeq.sortBy(_._2)
        winner = CompressionMode.none
        var medians = ratioSamples.mapValues(_.median(100.0f))
        while (candidates.nonEmpty) {
          val next = candidates.head._1
          candidates = candidates.tail
          if (medians(next) < medians(winner) - 0.03) {
            winner = next
          }
        }
        if (winner != winnerBackup) {
          statistics += winner -> (statistics.getOrElse(winner, 0) + 1)
          if (winnerBackup != null)
            statistics += winnerBackup -> (statistics.getOrElse(winnerBackup, 0) - 1)
          l.info( s"""Sampling for $ext is done and winner is $winner with ${medians(winner)}""")
        }
      }
    }

  }

  class SamplingBlock(val mode: CompressionMode, bIn: Block) {
    var block: Block = {
      val b = new Block(bIn.id, bIn.content)
      b.hash = bIn.hash
      b.mode = mode
      b
    }
    var resultArrived = false

    def ratio: Float = 1.0f * block.compressed.length / block.content.length
  }

  val extensions: mutable.HashMap[String, SamplingData] = mutable.HashMap[String, SamplingData]()

  def getSamplingData(ext: String): SamplingData =
    extensions.getOrElseUpdate(ext, new SamplingData(ext))

  def extension(block: Block): String = {
    val name = block.id.file.name
    if (name.contains('.'))
      name.drop(name.lastIndexOf('.') + 1)
    else
      name
  }

  def blockCompressed(block: Block, nanoTime: Long) {
    val toSave = mutable.Buffer[Block]()
    if (block.uncompressedLength == config.blockSize.bytes) {
      val set = speeds(block.mode)
      set.insertSorted(nanoTime)
    }
    val ext = extension(block)
    val data = getSamplingData(ext)
    toSave ++= data.blockWasCompressed(block)
    toSave.foreach { block =>
      universe.blockHandler.writeCompressedBlock(block)
    }
  }

  def compressBlockAsync(block: Block) {
    universe.scheduleTask { () =>
      val startAt = TimingUtil.getCpuTime
      val compressed = CompressedStream.compress(block.content, block.mode)
      block.compressed = compressed
      val duration = TimingUtil.getCpuTime - startAt
      universe.compressionDecider.blockCompressed(block, duration)
    }
  }

  def compressBlock(block: Block): Unit = {
    decideMode(block) match {
      case Some(mode) =>
        block.mode = mode
        compressBlockAsync(block)
      case None =>
        // Needs to be sampled
        getSamplingData(extension(block)).sampleFile(block).foreach {
          block =>
            compressBlockAsync(block)
        }
    }
  }

  def decideMode(block: Block): Option[CompressionMode] = {
    if (block.uncompressedLength < 256) {
      return Some(CompressionMode.none)
    }
    if (block.uncompressedLength < 2048) {
      return Some(CompressionMode.deflate)
    }
    val samplingData = getSamplingData(extension(block))
    if (samplingData.samplingFinished) {
      Some(samplingData.winner)
    } else {
      None
    }
  }

  def report() {
    if (statistics.values.sum > 0) {
      val stats = statistics.filter(_._2 > 0).mkString("\n")
      l.info(s"All algorithms with how often they were chosen:\n$stats")
    }
  }
}
