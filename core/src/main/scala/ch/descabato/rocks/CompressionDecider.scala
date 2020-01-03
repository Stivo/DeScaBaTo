package ch.descabato.rocks

import java.io.File

import ch.descabato.CompressionMode
import ch.descabato.core.config.BackupFolderConfiguration
import ch.descabato.utils.BytesWrapper
import ch.descabato.utils.CompressedBytes
import ch.descabato.utils.CompressedStream

object CompressionDeciders {
  def createForConfig(backupFolderConfiguration: BackupFolderConfiguration): CompressionDecider = {
    if (backupFolderConfiguration.compressor.isCompressionAlgorithm) {
      new SimpleCompressionDecider(backupFolderConfiguration.compressor)
    } else {
      // TODO new SamplingCompressionDecider()
      ???
    }
  }
}

trait CompressionDecider {

  def compressBlock(file: File, block: BytesWrapper): CompressedBytes = {
    CompressedStream.compressBytes(block, determineCompressor(file, block))
  }

  def report(): Unit = {}

  protected def determineCompressor(file: File, block: BytesWrapper): CompressionMode

}

class SimpleCompressionDecider(val mode: CompressionMode) extends CompressionDecider {
  override protected def determineCompressor(file: File, block: BytesWrapper): CompressionMode = mode
}

//class SamplingCompressionDecider extends CompressionDecider {
//  override def determineCompressor(file: File, block: BytesWrapper): CompressionMode = {
//
//  }
//}
