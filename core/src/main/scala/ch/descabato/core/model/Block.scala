package ch.descabato.core.model

import ch.descabato.CompressionMode
import ch.descabato.utils.BytesWrapper
import ch.descabato.utils.CompressedStream
import ch.descabato.utils.Hash
import com.typesafe.scalalogging.LazyLogging

case class Block(blockId: BlockId, private val content: BytesWrapper, hash: Hash) extends LazyLogging {

  logger.info(s"$blockId created with length ${content.length} and hash ${hash}")

  def contentLength: Long = content.length

  def compress(mode: CompressionMode): CompressedBlock = {
    val compressed = CompressedStream.compress(content, mode)
    CompressedBlock(blockId, compressed, hash, content.length)
  }

}

case class CompressedBlock(blockId: BlockId, compressed: BytesWrapper, hash: Hash, uncompressedLength: Int)