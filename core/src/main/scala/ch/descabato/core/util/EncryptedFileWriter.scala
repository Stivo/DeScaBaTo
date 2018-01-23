package ch.descabato.core.util

import java.io.File
import javax.crypto.{Cipher, CipherOutputStream}

import ch.descabato.utils.BytesWrapper
import ch.descabato.utils.Implicits._

class EncryptedFileWriter(file: File, passphrase: String) extends EncryptedFileWriterBase(file, passphrase) {

  writeHeader()
  startEncryptedPart()

  def write(bytes: BytesWrapper): Long = {
    val out = position
    outputStream.write(bytes)
    position += bytes.length
    out
  }

  private def startEncryptedPart(): Unit = {
    initializeCipher(Cipher.ENCRYPT_MODE, keyInfo)
    outputStream = new CipherOutputStream(outputStream, cipher)
    writeHmac()
  }

  override def finish(): Unit = {
    outputStream.close()
  }
}
