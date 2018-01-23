package ch.descabato.core.util

import java.io.{File, FileOutputStream, OutputStream}
import javax.crypto.{Cipher, CipherOutputStream}

import akka.util.ByteString
import ch.descabato.core_old.kvstore.{CryptoUtils, KeyInfo}
import ch.descabato.utils.BytesWrapper
import ch.descabato.utils.Implicits._

class EncryptedFileWriter(val file: File, val passphrase: String) extends CipherUser(passphrase) with FileWriter {

  private var position = 0L

  private var outputStream: OutputStream = new FileOutputStream(file)

  var header = ByteString.empty
  header ++= magicMarker
  header ++= kvStoreVersion
  header :+= 2.toByte // type 2 continues with encryption parameters

  private def writeEncryptionInfo() {
    header :+= encryptionInfo.algorithm
    header :+= encryptionInfo.macAlgorithm
    header :+= encryptionInfo.ivLength
    header ++= encryptionInfo.iv
  }

  private def writeKeyDerivationInfo() {
    header :+= keyDerivationInfo.algorithm
    header :+= keyDerivationInfo.iterationsPower
    header :+= keyDerivationInfo.memoryFactor
    header :+= keyDerivationInfo.keyLength
    header :+= keyDerivationInfo.saltLength
    header ++= keyDerivationInfo.salt
  }

  def write(bytes: BytesWrapper): Long = {
    val out = position
    outputStream.write(bytes)
    position += bytes.length
    out
  }

  override def currentPosition(): Long = position

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

  writeKeyDerivationInfo()
  writeEncryptionInfo()
  val keyHere = CryptoUtils.keyDerive(passphrase, keyDerivationInfo.salt,
    keyDerivationInfo.keyLength, keyDerivationInfo.iterationsPower, keyDerivationInfo.memoryFactor)
  startEncryptedPart(keyHere)

  override def finish(): Unit = {
    outputStream.close()
  }
}
