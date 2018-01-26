package ch.descabato.core.util

import java.io.{File, FileOutputStream}
import java.security.{DigestOutputStream, MessageDigest}

import ch.descabato.utils.Implicits._
import ch.descabato.utils.{BytesWrapper, Hash}

trait FileWriter {
  // ----------- Interface ---------------
  final def currentPosition(): Long = position

  def file: File

  def write(content: BytesWrapper): Long

  final def md5Hash(): Hash = _md5Hash

  final def finish(): Unit = {
    finishImpl()
    _md5Hash = Hash(outputStream.getMessageDigest.digest())
  }

  // ----------- Implementation ---------------
  protected val outputStream = new DigestOutputStream(new FileOutputStream(file), MessageDigest.getInstance("MD5"))

  protected def finishImpl(): Unit

  private var _md5Hash: Hash = Hash.empty

  protected var position = 0
}

class SimpleFileWriter(val file: File) extends FileWriter {

  override def write(content: BytesWrapper): Long = {
    val out = position
    outputStream.write(content)
    position += content.length
    out
  }

  override def finishImpl(): Unit = {
    outputStream.close()
  }

}