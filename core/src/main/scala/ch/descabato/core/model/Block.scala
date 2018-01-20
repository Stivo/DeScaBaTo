package ch.descabato.core.model

import akka.util.ByteString
import ch.descabato.core_old.{BackupConfigurationHandler, BackupFolderConfiguration}
import ch.descabato.frontend.BackupConf
import ch.descabato.utils.{BytesWrapper, CompressedStream, Hash}
import net.jpountz.lz4.LZ4Factory
import org.slf4j.LoggerFactory

case class Block(blockId: BlockId, var content: BytesWrapper, hash: Hash) {
  val logger = LoggerFactory.getLogger(getClass)

  var isAlreadySaved: Boolean = false

  var compressed: BytesWrapper = _

  def compress(config: BackupFolderConfiguration): Block = {
    compressed = CompressedStream.compress(content, config.compressor)
    this
  }

}
