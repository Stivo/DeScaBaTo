package ch.descabato.core.model

import ch.descabato.CompressionMode
import ch.descabato.utils.{BytesWrapper, CompressedStream, Hash}

case class Block(blockId: BlockId, private val content: BytesWrapper, hash: Hash) {

  def contentLength: Long = content.length

  def compress(mode: CompressionMode): CompressedBlock = {
    val compressed = CompressedStream.compress(content, mode)
    CompressedBlock(blockId, compressed, hash, content.length)
  }

}

case class CompressedBlock(blockId: BlockId, compressed: BytesWrapper, hash: Hash, uncompressedLength: Int) {
  def decompress(): Block = {
    val uncompressed = CompressedStream.decompress(compressed)
    Block(blockId, compressed, hash)
  }
}