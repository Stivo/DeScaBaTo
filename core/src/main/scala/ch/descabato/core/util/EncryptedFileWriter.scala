package ch.descabato.core.util

import java.io.File
import javax.crypto.{Cipher, CipherOutputStream}

import ch.descabato.utils.BytesWrapper
import ch.descabato.utils.Implicits._

class EncryptedFileWriter(file: File, passphrase: String, keylength: Int) extends EncryptedFileWriterBase(file, passphrase, keylength) {

  writeHeader()
  startEncryptedPart()
  var cipherOutputStream: CipherOutputStream = _

  def write(bytes: BytesWrapper): Long = {
    val out = position
    cipherOutputStream.write(bytes)
    position += bytes.length
    out
  }

  private def startEncryptedPart(): Unit = {
    initializeCipher(Cipher.ENCRYPT_MODE, keyInfo)
    cipherOutputStream = new CipherOutputStream(outputStream, cipher)
    writeHmac()
  }

  override def finishImpl(): Unit = {
    cipherOutputStream.close()
  }
}
