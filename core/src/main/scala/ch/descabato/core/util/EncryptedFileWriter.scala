package ch.descabato.core.util

import java.io.{File, FileOutputStream, OutputStream}
import javax.crypto.{Cipher, CipherOutputStream}

import ch.descabato.utils.Implicits._
import akka.util.ByteString
import ch.descabato.core_old.kvstore.{CryptoUtils, KeyInfo}
import ch.descabato.utils.BytesWrapper

class EncryptedFileWriter(val file: File, key: Array[Byte]) extends CipherUser(key) with FileWriter {

  private var position = 0L

  private var outputStream: OutputStream = new FileOutputStream(file)

  var header = ByteString.empty
  header ++= magicMarker
  header ++= kvStoreVersion
  header :+= 1.toByte // type 1 continues with encryption parameters

  private def writeEncryptionInfo() {
    header :+= encryptionInfo.algorithm
    header :+= encryptionInfo.macAlgorithm
    header :+= encryptionInfo.ivLength
    header ++= encryptionInfo.iv
  }

  def write(bytes: BytesWrapper): Long = {
    val out = position
    outputStream.write(bytes)
    position += bytes.length
    out
  }

  private def startEncryptedPart(key: Array[Byte]): Unit = {
    outputStream.write(header.toArray)
    outputStream.flush()
    position += header.length
    keyInfo = new KeyInfo(key)
    keyInfo.iv = encryptionInfo.iv
    encryptionBoundary = position
    initializeCipher(Cipher.ENCRYPT_MODE, keyInfo)
    outputStream = new CipherOutputStream(outputStream, cipher)
    write(CryptoUtils.hmac(header.toArray, keyInfo).wrap())
  }

  writeEncryptionInfo()
  startEncryptedPart(key)

  override def finish(): Unit = {
    outputStream.close()
  }
}
