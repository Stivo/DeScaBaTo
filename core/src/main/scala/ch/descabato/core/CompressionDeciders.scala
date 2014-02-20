package ch.descabato.core

import ch.descabato.CompressionMode
import ch.descabato.utils.Implicits._
import scala.collection.mutable
import scala.language.implicitConversions
import ch.descabato.utils.Utils

class SimpleCompressionDecider extends CompressionDecider with UniversePart {
  def blockCompressed(block: Block, nanoTime: Long) {
    universe.cpuTaskHandler.makeZipEntry(block)
  }
  def compressBlock(block: Block): Unit = {
    block.mode = universe.config.compressor
    universe.cpuTaskHandler.compress(block)
  }
  def report() {
    
  }
}

object StatisticHelper {

  var defaultLimit = 101

  implicit def longbuf2sortedbuf(buf: mutable.Buffer[Long]) = {
    new SortedBuffer[Long](buf)
  }

  implicit def floatbuf2sortedbuf(buf: mutable.Buffer[Float]) = {
    new SortedBuffer[Float](buf)
  }

  implicit class SortedBuffer[T : Numeric](buf: mutable.Buffer[T])(implicit num: Numeric[T]) {
    def median(default: T): T = {
      if (buf.isEmpty) {
        default
      } else {
        val middle = buf.size / 2
        buf(middle)
      }
    }

    def insertSorted(x: T) {
      if (buf.size > 2*defaultLimit)
        return
      buf += x
      if (buf.size == 1)
        return
      // insertion sort in place
      var i = buf.size - 2
      while (i >= 0) {
        if (num.lt(buf(i+1),buf(i))) {
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

class SmartCompressionDecider extends CompressionDecider with UniversePart with Utils {
  import StatisticHelper._

  val samples = 31

  val algos = CompressionMode.values.filter(_.isCompressionAlgorithm()).sortBy(_.getByte)
  
  val speeds = algos.map(x => mutable.Buffer[Long](x.getEstimatedTime)).toArray

  val statistics = algos.map(x => 0)

  /**
   * Collects sampling data for one extension.
   * Once enough samples are in a winner is declared, until then
   * each block is sent to be compressed with each mode and results compared.
   */
  class SamplingData(val ext: String) {
    val ratioSamples = algos.map(x => mutable.Buffer[Float]())
    val samplingBlocks = mutable.HashMap[(Int, String), Array[SamplingBlock]]()
    def idForBlock(b: Block) = (b.id.part, b.id.file.path)
    
    def sampleFile(block: Block) = {
      // create new blocks with each algorithm
      val samples = algos.map(x => new SamplingBlock(x, block)).sortBy(_.mode.getByte())
      samplingBlocks += (idForBlock(block)) -> samples
      samples.map(_.block)
    }
    
    def samplingFinished = ratioSamples.head.size > samples
    
    def blockWasCompressed(b: Block): Option[Block] = {
      val id = idForBlock(b)
      if (!(samplingBlocks safeContains id)) {
        Some(b)
      } else {
        val sample = samplingBlocks(id)
        sample(b.mode.getByte()).block = b
        sample(b.mode.getByte()).resultArrived = true
        if (sample.forall(_.resultArrived)) {
          sample.foreach { s =>
            ratioSamples(s.mode.getByte()) insertSorted s.ratio
          }
          samplingBlocks -= idForBlock(b)
          checkIfSamplingDone()
          Some(sample.minBy(_.ratio).block)
        } else {
          None
        }
      }
    }

    var winner: CompressionMode = null

    def checkIfSamplingDone() {
      if (samplingFinished) {
        val winnerBackup = winner
        // candidates are all algos
        var candidates = algos.zip(speeds.map(_.median(config.blockSize.bytes * 10))).sortBy(_._2).drop(1)
        winner = CompressionMode.none
        val medians = algos.map(x => ratioSamples(x.getByte).median(100.0f))
        while (!candidates.isEmpty) {
          val next = candidates.head._1
          candidates = candidates.tail
          if (medians(next.getByte) < medians(winner.getByte) - 0.03) {
            winner = next
          }
        }
        if (winner != winnerBackup) {
          statistics(winner.getByte) += 1
          if (winnerBackup != null)
            statistics(winnerBackup.getByte) -= 1
          val ratiosAndNames = algos.zip(medians).sortBy(_._2).mkString(", ")
          val speedsAndNames = algos.zip(speeds.map(_.median(config.blockSize.bytes * 10))).sortBy(_._2).mkString(", ")
          l.info( s"""Sampling for $ext is done and winner is $winner with ${ratioSamples(winner.getByte).median(100)},
ratios are $ratiosAndNames,
speeds are $speedsAndNames""")
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
    def ratio = 1.0f * block.compressed.remaining() / block.content.length
  }
  
  val extensions = mutable.HashMap[String, SamplingData]() 
    //algos.map(_ => SortedSet[Long]()).toArray
  
  def getSamplingData(ext: String) = 
    extensions.getOrElseUpdate(ext, new SamplingData(ext))
  
  def extension(block: Block): Option[String] = {
    val name = block.id.file.name
    if (name.contains('.'))
      Some(name.drop(name.lastIndexOf('.')+1))
    else
      None
  }
  
  def blockCompressed(block: Block, nanoTime: Long) {
    val toSave = mutable.Buffer[Block]()
    if (block.uncompressedLength == config.blockSize.bytes) {
      val set = speeds(block.header)
      set insertSorted (nanoTime)
    }
    val ext = extension(block)
    if (ext.isDefined) {
      val data = getSamplingData(ext.get)
      toSave ++= data.blockWasCompressed(block)
    } else {
      toSave += block
    }
    toSave.foreach { block =>
      universe.cpuTaskHandler.makeZipEntry(block)
    }
  }
  
  def compressBlock(block: Block): Unit = {
    decideMode(block) match {
      case Some(mode) =>
        block.mode = mode
        universe.cpuTaskHandler.compress(block)
      case None =>
        // Needs to be sampled
        getSamplingData(extension(block).get).sampleFile(block).foreach { 
          block =>
          	universe.cpuTaskHandler.compress(block)
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
    extension(block) match {
      // No extension, just use best algorithm
      case None => 
        return Some(CompressionMode.bzip2)
      case Some(ext) =>
        val samplingData = getSamplingData(ext) 
        if (samplingData.samplingFinished) {
          return Some(samplingData.winner) 
        }
    }
    None
  }
    
  def report() {
    if (statistics.sum > 0) {
      val stats = algos.zip(statistics).mkString("\n")
      l.info(s"All algorithms with how often they were chosen:\n$stats")
    }
  }
}
