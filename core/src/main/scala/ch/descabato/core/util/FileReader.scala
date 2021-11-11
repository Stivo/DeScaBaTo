package ch.descabato.core.util

import ch.descabato.utils.BytesWrapper

import java.io.File
import java.io.RandomAccessFile

trait FileReader extends AutoCloseable {

  def file: File

  def readAllContent(): BytesWrapper

  def readChunk(position: Long, length: Long): BytesWrapper

  def close(): Unit

  def startOfContent: Long

}

class SimpleFileReader(val file: File) extends FileReader {
  private val randomAccessReader = new RandomAccessFile(file, "r")
  var position = 0

  override def readAllContent(): BytesWrapper = {
    readChunk(0, file.length())
  }

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