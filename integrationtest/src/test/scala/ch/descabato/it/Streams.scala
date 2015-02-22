package ch.descabato.it

import java.io.{InputStream, OutputStream}

object Streams {

  class DelegatingInputStream(in: InputStream) extends InputStream {
    def read() = in.read()
    override def read(b: Array[Byte], start: Int, len: Int) = in.read(b, start, len)
    override def close() = in.close()
    override def mark(limit: Int) = in.mark(limit)
    override def reset() = in.reset()
    override def markSupported() = in.markSupported()
  }

  class DelegatingOutputStream(out: OutputStream*) extends OutputStream {
    def write(b: Int) = out foreach(_.write(b))
    override def write(b: Array[Byte], start: Int, len: Int) = out foreach(_.write(b, start, len))
    override def close() = out foreach(_.close())
    override def flush() = out foreach(_.flush())
  }

}
