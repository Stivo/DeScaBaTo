package ch.descabato.utils

import ch.descabato.CustomByteArrayOutputStream
import ch.descabato.core.util.Constants
import ch.descabato.hashes.BuzHash

import java.io.InputStream
import java.io.OutputStream


object Streams extends Utils {

  abstract class ChunkingOutputStream(func: BytesWrapper => _, private val initialSize: Int = 100 * 1024) extends OutputStream {

    var out = new CustomByteArrayOutputStream(initialSize)

    var funcWasCalledOnce = false

    var oneByte: Array[Byte] = Array.ofDim[Byte](1)

    final def write(b: Int): Unit = {
      oneByte(0) = b.asInstanceOf[Byte]
      write(oneByte)
    }

    def createChunkNow(): Unit = {
      funcWasCalledOnce = true
      func(out.toBytesWrapper())
      out.reset()
    }

    override final def close(): Unit = {
      if (out.size() > 0 || !funcWasCalledOnce) {
        createChunkNow()
      }
      super.close()
    }

    def currentChunkSize: Int = out.size()

  }

  class VariableBlockOutputStream(func: (BytesWrapper => _),
                                  val minBlockSize: Int = Constants.Chunking.minBlockSize,
                                  val maxBlockSize: Int = Constants.Chunking.maxBlockSize,
                                  val bitsToChunkOn: Byte = Constants.Chunking.bitmask) extends ChunkingOutputStream(func, minBlockSize) {

    private val buzHash = new BuzHash(Constants.Chunking.buzhashSize)

    private val noBuzhashNeeded = minBlockSize - Constants.Chunking.buzhashSize * 2

    override def write(buf: Array[Byte], start: Int, len: Int): Unit = {
      if (currentChunkSize < noBuzhashNeeded) {
        val writeDirectly = Math.min(noBuzhashNeeded - currentChunkSize, len)
        out.write(buf, start, writeDirectly)
        writeNormal(buf, start + writeDirectly, len - writeDirectly)
      } else {
        writeNormal(buf, start, len)
      }
    }

    private def writeNormal(buf: Array[Byte], start: Int, len: Int): Unit = {
      val end = start + len
      var pos = start
      while (pos < end) {
        val bytesToFindBoundaryIn = computeBytesToRead(end, pos)
        val boundary = buzHash.updateAndReportBoundary(buf, pos, bytesToFindBoundaryIn, bitsToChunkOn)
        if (boundary < 0) {
          // no boundary found
          out.write(buf, pos, bytesToFindBoundaryIn)
          pos += bytesToFindBoundaryIn
          if (currentChunkSize == maxBlockSize) {
            createChunkNow()
          }
        } else {
          out.write(buf, pos, boundary)
          pos += boundary
          if (currentChunkSize >= minBlockSize) {
            // boundary found and more than minimum block size
            createChunkNow()
          }
        }
      }
    }

    override def createChunkNow(): Unit = {
      super.createChunkNow()
      buzHash.reset()
    }

    private def computeBytesToRead(end: Int, pos: Int): Int = {
      val remainingBytes = end - pos
      val bytesBeforeMaxBlockSize = maxBlockSize - currentChunkSize
      Math.min(bytesBeforeMaxBlockSize, remainingBytes)
    }
  }

  class DelegatingInputStream(in: InputStream) extends InputStream {
    def read(): Int = in.read()

    override def read(b: Array[Byte], start: Int, len: Int): Int = in.read(b, start, len)

    override def close(): Unit = in.close()

    override def mark(limit: Int): Unit = in.mark(limit)

    override def reset(): Unit = in.reset()

    override def markSupported(): Boolean = in.markSupported()

    override def available(): Int = in.available()

    override def skip(n: Long): Long = in.skip(n)
  }

  class DelegatingOutputStream(out: OutputStream*) extends OutputStream {
    def write(b: Int): Unit = out foreach (_.write(b))

    override def write(b: Array[Byte], start: Int, len: Int): Unit = out foreach (_.write(b, start, len))

    override def close(): Unit = out foreach (_.close())

    override def flush(): Unit = out foreach (_.flush())
  }

}
