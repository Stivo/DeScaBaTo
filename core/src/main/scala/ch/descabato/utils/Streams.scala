package ch.descabato.utils

import java.io.{InputStream, OutputStream}

import ch.descabato.CustomByteArrayOutputStream
import ch.descabato.hashes.RollingBuzHash


object Streams extends Utils {

  class BlockOutputStream(val blockSize: Int, func: (BytesWrapper => _)) extends OutputStream {

    var out = new CustomByteArrayOutputStream(blockSize)

    var funcWasCalledOnce = false

    def write(b: Int) {
      out.write(b)
      handleEnd()
    }

    def handleEnd() {
      if (cur == blockSize) {
        funcWasCalledOnce = true
        func(out.toBytesWrapper())
        out.reset()
      }
    }

    def cur: Int = out.size()

    override def write(buf: Array[Byte], start: Int, len: Int) {
      var (lenC, startC) = (len, start)
      while (lenC > 0) {
        val now = Math.min(blockSize - cur, lenC)
        out.write(buf, startC, now)
        lenC -= now
        startC += now
        handleEnd()
      }
    }

    override def close() {
      if (out.size() > 0 || !funcWasCalledOnce)
        func(out.toBytesWrapper())
      super.close()
      // out was returned to the pool, so remove instance pointer
      out = null
    }

  }

  class VariableBlockOutputStream(val maxBlockSize: Int, func: (BytesWrapper => _)) extends OutputStream {

    var out = new CustomByteArrayOutputStream(maxBlockSize)

    var funcWasCalledOnce = false

    val onebyte = Array.ofDim[Byte](1)

    val buzhash = new RollingBuzHash()

    def write(b: Int) {
      onebyte(0) = b.asInstanceOf[Byte]
      write(onebyte)
    }

    def handleEnd() {
      funcWasCalledOnce = true
      func(out.toBytesWrapper())
      out.reset()
    }

    def cur: Int = out.size()

    override def write(buf: Array[Byte], start: Int, len: Int) {
      val end = start + len
      var pos = start
      while (pos < end) {
        val boundary = buzhash.updateAndReportBoundary(buf, pos, end - pos, 20)
        // no boundary found, try again
        if (boundary < 0) {
          out.write(buf, pos, len - pos)
          pos = end
        } else {
          // boundary found
          out.write(buf, pos, boundary)
          handleEnd()
          pos += boundary
        }
      }
    }

    override def close() {
      if (out.size() > 0 || !funcWasCalledOnce) {
        func(out.toBytesWrapper())
      }
      super.close()
      // out was returned to the pool, so remove instance pointer
      out = null
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
    def write(b: Int) = out foreach(_.write(b))
    override def write(b: Array[Byte], start: Int, len: Int) = out foreach(_.write(b, start, len))
    override def close() = out foreach(_.close())
    override def flush() = out foreach(_.flush())
  }

}
