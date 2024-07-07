package ch.descabato.core.actions

import ch.descabato.CompressionMode
import ch.descabato.core.config.BackupFolderConfiguration
import ch.descabato.utils.BytesWrapper
import ch.descabato.utils.CompressedBytes
import ch.descabato.utils.CompressedStream
import ch.descabato.utils.Implicits.*
import ch.descabato.utils.StandardMeasureTime
import ch.descabato.utils.Utils

import java.io.File

object CompressionDeciders {
  def createForConfig(backupFolderConfiguration: BackupFolderConfiguration): CompressionDecider = {
    if (backupFolderConfiguration.compressor.isCompressionAlgorithm) {
      new SimpleCompressionDecider(backupFolderConfiguration.compressor)
    } else {
      new SamplingCompressionDecider()
    }
  }
}

trait CompressionDecider {

  def compressBlock(file: File, block: BytesWrapper): CompressedBytes = {
    val (mode, compressedBytes) = determineCompressor(file, block)
    compressedBytes match {
      case Some(b) =>
        b
      case None =>
        CompressedStream.compressBytes(block, mode)
    }
  }

  def report(): Unit = {}

  protected def determineCompressor(file: File, block: BytesWrapper): (CompressionMode, Option[CompressedBytes])

}

class SimpleCompressionDecider(val mode: CompressionMode) extends CompressionDecider {
  override protected def determineCompressor(file: File, block: BytesWrapper): (CompressionMode, Option[CompressedBytes]) = {
    (mode, None)
  }
}

class SamplingCompressionDecider extends CompressionDecider {
  var files: Map[File, Sampling] = Map.empty

  override def determineCompressor(file: File, block: BytesWrapper): (CompressionMode, Option[CompressedBytes]) = {
    if (block.length < 128) {
      (CompressionMode.none, None)
    } else if (file.length() > 1 * 1024 * 1024) {
      // only sample above 1Mb, so it is worth it
      sampleFile(file, block)
    } else {
      (CompressionMode.zstd1, None)
    }
  }

  def sampleFile(file: File, block: BytesWrapper): (CompressionMode, Option[CompressedBytes]) = {
    if (!files.safeContains(file)) {
      files += file -> new Sampling(file)
    }
    files(file).decide(block)
  }

}

class Sampling(file: File) extends Utils {
  var samples: Seq[SamplingData] = Seq.empty

  var decision: Option[CompressionMode] = None

  def setDecisionAndReturn(algorithm: CompressionMode): (CompressionMode, Option[CompressedBytes]) = {
    decision = Some(algorithm)
    logger.debug(s"Decision for $file is $algorithm")
    (decision.get, None)
  }

  def decide(block: BytesWrapper): (CompressionMode, Option[CompressedBytes]) = {
    if (decision.nonEmpty) {
      (decision.get, None)
    } else {
      val (data, compressed) = sample(CompressionMode.zstd9, block)
      if (samples.head.ratio < 0.98) {
        setDecisionAndReturn(CompressionMode.zstd9)
        (CompressionMode.zstd9, Some(compressed))
      } else {
        setDecisionAndReturn(CompressionMode.none)
      }
    }
  }

  def sample(mode: CompressionMode, block: BytesWrapper): (SamplingData, CompressedBytes) = {
    val measure = new StandardMeasureTime()
    val compressed = CompressedStream.compressBytes(block, mode)
    val sample = new SamplingData(mode, block.length, compressed.compressed.length, measure.timeInMs())
    samples :+= sample
    logger.debug(s"Sampling $mode for $file for block with length ${
      block.length
    }, had ratio ${
      sample.ratio
    }")
    (sample, compressed)
  }

}

class SamplingData(val algorithm: CompressionMode, val uncompressedLength: Int, val compressedLength: Int, val timeMs: Long) {
  def ratio: Double = compressedLength * 1.0 / uncompressedLength

  override def toString: String = s"$algorithm: ${ratio} in $timeMs ms"
}