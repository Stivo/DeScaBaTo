package ch.descabato.utils

import java.io.{InputStream, OutputStream}

import ch.descabato.CustomByteArrayOutputStream
import ch.descabato.hashes.RollingBuzHash


object Streams extends Utils {

  abstract class ChunkingOutputStream(func: BytesWrapper => _) extends OutputStream  {
    var out = new CustomByteArrayOutputStream()

    var funcWasCalledOnce = false

    var oneByte = Array.ofDim[Byte](1)

    final def write(b: Int) {
      oneByte(0) = b.asInstanceOf[Byte]
      out.write(oneByte)
    }

    def createChunkNow() {
      funcWasCalledOnce = true
      func(out.toBytesWrapper())
      logger.info(s"Created chunk with ${readableFileSize(out.size())}")
      out.reset()
    }

    override final def close() {
      if (out.size() > 0 || !funcWasCalledOnce)
        func(out.toBytesWrapper())
      super.close()
    }

    def currentChunkSize: Int = out.size()

  }

  class BlockOutputStream(val blockSize: Int, func: (BytesWrapper => _)) extends ChunkingOutputStream(func) {

    def handleEnd() {
      if (currentChunkSize == blockSize) {
        createChunkNow()
      }
    }


    override def write(buf: Array[Byte], start: Int, len: Int) {
      var (lenC, startC) = (len, start)
      while (lenC > 0) {
        val now = Math.min(blockSize - currentChunkSize, lenC)
        out.write(buf, startC, now)
        lenC -= now
        startC += now
        handleEnd()
      }
    }


  }

  class VariableBlockOutputStream(val maxBlockSize: Int, func: (BytesWrapper => _), val bitsToChunkOn: Byte = 20) extends ChunkingOutputStream(func) {

    val buzhash = new RollingBuzHash()

    override def write(buf: Array[Byte], start: Int, len: Int) {
      val end = start + len
      var pos = start
      while (pos < end) {
        val remainingBytes = end - pos
        val bytesBeforeMaxBlockSize = maxBlockSize - pos
        val bytesToFindBoundaryIn = Math.min(bytesBeforeMaxBlockSize, remainingBytes)
        val boundary = buzhash.updateAndReportBoundary(buf, pos, bytesToFindBoundaryIn, bitsToChunkOn)
        if (boundary < 0) {
          // no boundary found
          out.write(buf, pos, remainingBytes)
          pos = end
          if (currentChunkSize == maxBlockSize) {
            createChunkNow()
          }
        } else {
          // boundary found
          out.write(buf, pos, boundary)
          createChunkNow()
          pos += boundary
        }
      }
    }

  }

  class DelegatingInputStream(in: InputStream) extends InputStream {
    def read() = in.read()

    override def read(b: Array[Byte], start: Int, len: Int) = in.read(b, start, len)

    override def close() = in.close()

    override def mark(limit: Int) = in.mark(limit)

    override def reset() = in.reset()

    override def markSupported() = in.markSupported()

    override def available(): Int = in.available()

    override def skip(n: Long): Long = in.skip(n)
  }

  class DelegatingOutputStream(out: OutputStream*) extends OutputStream {
    def write(b: Int) = out foreach (_.write(b))

    override def write(b: Array[Byte], start: Int, len: Int) = out foreach (_.write(b, start, len))

    override def close() = out foreach (_.close())

    override def flush() = out foreach (_.flush())
  }

}
