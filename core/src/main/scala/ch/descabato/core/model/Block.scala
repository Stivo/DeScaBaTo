package ch.descabato.core.model

import ch.descabato.CompressionMode
import ch.descabato.utils.{ByteArrayPool, BytesWrapper, CompressedStream, Hash}

case class Block(blockId: BlockId, private val content: BytesWrapper, hash: Hash) {

  def contentLength: Long = content.length

  def compress(mode: CompressionMode): CompressedBlock = {
    val compressed = CompressedStream.compress(content, mode)
    CompressedBlock(blockId, compressed, hash, content.length)
  }

  override def finalize(): Unit = {
    try {
      ByteArrayPool.recycle(content)
    } catch {
      case _: Throwable => // ignore
    }
    super.finalize()
  }

}

case class CompressedBlock(blockId: BlockId, compressed: BytesWrapper, hash: Hash, uncompressedLength: Int) {

  override def finalize(): Unit = {
    try {
      ByteArrayPool.recycle(compressed)
    } catch {
      case _: Throwable  => // ignore
    }
    super.finalize()
  }

}