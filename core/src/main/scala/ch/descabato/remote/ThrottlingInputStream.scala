package ch.descabato.remote

import java.io.FileInputStream

import ch.descabato.utils.Streams.DelegatingInputStream

class ThrottlingInputStream(private val stream: FileInputStream, private val context: Option[RemoteOperationContext])
  extends DelegatingInputStream(stream) {

  override def read(): Int = {
    readBytes(1)
    stream.read()
  }

  private def readBytes(read: Int): Unit = {
    context.foreach { x =>
      x.rateLimiter.acquire(read)
      x.progress.current += read
    }
  }

  override def read(b: Array[Byte], off: Int, len: Int): Int = {
    val read = stream.read(b, off, len)
    readBytes(read)
    read
  }

}
