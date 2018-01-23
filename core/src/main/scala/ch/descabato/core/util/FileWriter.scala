package ch.descabato.core.util

import java.io.{File, FileOutputStream}

import ch.descabato.utils.BytesWrapper
import ch.descabato.utils.Implicits._

trait FileWriter {

  def currentPosition(): Long

  def file: File

  def write(content: BytesWrapper): Long

  def finish(): Unit

}

class SimpleFileWriter(val file: File) extends FileWriter {

  private val stream = new FileOutputStream(file)
  private var position = 0

  override def currentPosition(): Long = position

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