package ch.descabato.core

import java.io.{BufferedInputStream, File, FileInputStream}

import ch.descabato.utils.BytesWrapper
import ch.descabato.utils.Streams.VariableBlockOutputStream


class FileIterator(file: File) extends Iterator[BytesWrapper] {
  private val in = new BufferedInputStream(new FileInputStream(file))
  private val byteBuffer: Array[Byte] = Array.ofDim[Byte](100 * 1024)

  private var buffer: Seq[BytesWrapper] = Seq.empty
  var out = new VariableBlockOutputStream({ chunk: BytesWrapper =>
    buffer :+= chunk
  })

  var closed = false

  private def tryToFillBuffer() = {
    if (!closed) {
      var lastRead = 1
      do {
        lastRead = in.read(byteBuffer)
        if (lastRead >= 0) {
          out.write(byteBuffer, 0, lastRead)
        }
      } while (lastRead >= 0 && buffer.isEmpty)
      if (lastRead < 0) {
        closed = true
        in.close()
        out.close()
      }
    }
  }

  override def hasNext: Boolean = {
    if (buffer.isEmpty) {
      tryToFillBuffer()
    }
    buffer.nonEmpty
  }

  override def next(): BytesWrapper = {
    val out = buffer.head
    buffer = buffer.tail
    out
  }
}
