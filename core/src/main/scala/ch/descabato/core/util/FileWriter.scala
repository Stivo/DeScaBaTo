package ch.descabato.core.util

import ch.descabato.utils.BytesWrapper
import ch.descabato.utils.Hash
import ch.descabato.utils.Implicits._

import java.io.File
import java.io.FileOutputStream
import java.io.OutputStream
import java.security.DigestOutputStream
import java.security.MessageDigest

trait FileWriter extends AutoCloseable {
  // ----------- Interface ---------------
  final def currentPosition(): Long = position

  def file: File

  def write(content: BytesWrapper): Long

  final def md5Hash(): Hash = _md5Hash

  def asOutputStream(): OutputStream = {
    new OutputStream {
      override def write(b: Int): Unit = ???

      override def write(b: Array[Byte], off: Int, len: Int): Unit = write(BytesWrapper(b, off, len))

      override def close(): Unit = FileWriter.this.close()
    }
  }

  override final def close(): Unit = {
    finishImpl()
    _md5Hash = Hash(outputStream.getMessageDigest.digest())
  }

  // ----------- Implementation ---------------
  file.getParentFile.mkdirs()
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