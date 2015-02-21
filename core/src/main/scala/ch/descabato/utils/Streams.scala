package ch.descabato.utils

import java.io.OutputStream

import ch.descabato.ByteArrayOutputStream


object Streams extends Utils {

  class BlockOutputStream(val blockSize: Int, func: (BytesWrapper => _)) extends OutputStream {

    var out = new ByteArrayOutputStream(blockSize)

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

    def cur = out.size()

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

}
