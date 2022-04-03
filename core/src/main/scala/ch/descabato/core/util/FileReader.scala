package ch.descabato.core.util

import ch.descabato.utils.BytesWrapper

import java.io.File
import java.io.InputStream
import java.io.RandomAccessFile

trait FileReader extends InputStream with AutoCloseable {

  def file: File

  def readAllContent(): BytesWrapper = readChunk(startOfContent, file.length - startOfContent)

  def readChunk(position: Long, length: Long): BytesWrapper

  def close(): Unit

  def startOfContent: Long

  private var streamReaderPosition: Long = -1

  override def read(b: Array[Byte], off: Int, len: Int): Int = {
    if (streamReaderPosition == -1) {
      streamReaderPosition = startOfContent
    }
    val toRead = Math.min(len, file.length() - streamReaderPosition).toInt
    if (toRead > 0) {
      readChunk(streamReaderPosition, toRead).copyToArray(b, off, toRead)
      streamReaderPosition += toRead
    }
    toRead
  }

  override def read(): Int = {
    // since this should always be wrapped in a buffered input stream, this method should never be called
    // also not sure how to implement it correctly
    ???
  }

}

class SimpleFileReader(val file: File) extends FileReader {
  private val randomAccessReader = new RandomAccessFile(file, "r")
  var position = 0

  override def readChunk(position: Long, length: Long): BytesWrapper = {
    randomAccessReader.seek(position)
    val value = Array.ofDim[Byte](length.toInt)
    randomAccessReader.readFully(value)
    BytesWrapper(value)
  }

  override def close(): Unit = {
    randomAccessReader.close()
  }

  override def startOfContent: Long = 0
}