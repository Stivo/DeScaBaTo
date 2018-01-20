package ch.descabato.core.util

import java.io.{File, FileOutputStream}

import akka.util.ByteString
import ch.descabato.utils.BytesWrapper
import ch.descabato.utils.Implicits._

trait FileWriter {

  def file: File

  def write(content: BytesWrapper): Long

  def finish(): Unit

}

class SimpleFileWriter(val file: File) extends FileWriter {

  private val stream = new FileOutputStream(file)
  var position = 0

  override def write(content: BytesWrapper): Long = {
    val out = position
    stream.write(content)
    position += content.length
    out
  }

  override def finish(): Unit = {
    stream.close()
  }
}