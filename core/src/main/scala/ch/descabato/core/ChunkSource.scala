package ch.descabato.core

import java.io.{BufferedInputStream, File, FileInputStream}

import ch.descabato.CustomByteArrayOutputStream
import ch.descabato.hashes.BuzHash
import ch.descabato.utils.BytesWrapper
import ch.descabato.utils.Streams.VariableBlockOutputStream
import org.apache.commons.compress.utils.IOUtils

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._

object FileIterator {

  val minBlockSize = 256 * 1024
  val maxBlockSize = 16 * 1024 * 1024
  val bits: Byte = 20

  def apply(file: File)(implicit executionContext: ExecutionContext): Iterator[BytesWrapper] = {
    if (file.length() == 0) {
      List.empty.iterator
    } else if (file.length() < minBlockSize) {
      new LazyFileReader(file)
    } else {
      new FastFileIterator(file)
    }
  }

}

class LazyFileReader(file: File)(implicit executionContext: ExecutionContext) extends Iterator[BytesWrapper] {
  val fut = Future {
    val stream = new FileInputStream(file)
    val bytes = Array.ofDim[Byte](file.length().toInt)
    IOUtils.readFully(stream, bytes)
    stream.close()
    BytesWrapper(bytes)
  }

  var count = 0
  override def hasNext: Boolean = {
    count < 1
  }

  override def next(): BytesWrapper = {
    count += 1
    Await.result(fut, 1.minute)
  }
}

class FastFileIterator(file: File,
                       val minBlockSize: Int = FileIterator.minBlockSize,
                       val maxBlockSize: Int = FileIterator.maxBlockSize,
                       var bits: Byte = FileIterator.bits) extends Iterator[BytesWrapper] {
  private val in = new BufferedInputStream(new FileInputStream(file), minBlockSize)
  private val byteBuffer: Array[Byte] = Array.ofDim[Byte](minBlockSize)
  private val buzHash: BuzHash = new BuzHash(64)

  private var buffer: Seq[BytesWrapper] = Seq.empty
  private val out = new CustomByteArrayOutputStream(minBlockSize * 4)
  private var inputFinished = false

  private def loadSomeBytes() = {
    val i = in.read(byteBuffer)
    if (i < 0) {
      inputFinished = true
      in.close()
      if (out.size() > 0) {
        createChunkNow()
      }
      out.close()
    }
    i
  }

  private def currentChunkSize: Int = out.size()

  private def computeBytesToRead(end: Int, pos: Int) = {
    val remainingBytes = end - pos
    val bytesBeforeMaxBlockSize = maxBlockSize - currentChunkSize
    Math.min(bytesBeforeMaxBlockSize, remainingBytes)
  }

  private def write(buf: Array[Byte], start: Int, len: Int) {
    val end = start + len
    var pos = start
    while (pos < end) {
      val bytesToFindBoundaryIn = computeBytesToRead(end, pos)
      val boundary = buzHash.updateAndReportBoundary(buf, pos, bytesToFindBoundaryIn, bits)
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

  private def createChunkNow(): Unit = {
    buffer :+= out.toBytesWrapper
    out.reset()
  }

  private def tryToFillBuffer() = {
    while (buffer.isEmpty && !inputFinished) {
      // get some more bytes
      val endOfCurrentBuffer = loadSomeBytes()
      write(byteBuffer, 0, endOfCurrentBuffer)
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
