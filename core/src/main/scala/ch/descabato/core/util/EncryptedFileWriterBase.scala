package ch.descabato.core.util

import ch.descabato.utils.BytesWrapper
import ch.descabato.utils.Utils

import java.io.File

abstract class EncryptedFileWriterBase(val file: File, val passphrase: String, keylength: Int) extends CipherUser with FileWriter with Utils {

  lazy val keyDerivationInfo = new KeyDerivationInfo(keyLength = (keylength / 8).toByte)

  protected val key: Array[Byte] = CryptoUtils.keyDerive(passphrase, keyDerivationInfo.salt,
    keyDerivationInfo.keyLength, keyDerivationInfo.iterationsPower, keyDerivationInfo.memoryFactor)

  private var header = Array.empty[Byte]
  header ++= magicMarker
  header ++= kvStoreVersion
  header :+= 2.toByte // type 2 continues with encryption parameters

  private def writeEncryptionInfo(): Unit = {
    header ++= Array(
      encryptionInfo.algorithm,
      encryptionInfo.macAlgorithm,
      encryptionInfo.ivLength
    )
    header ++= encryptionInfo.iv
  }

  private def writeKeyDerivationInfo(): Unit = {
    header ++= Array(
      keyDerivationInfo.algorithm,
      keyDerivationInfo.iterationsPower,
      keyDerivationInfo.memoryFactor,
      keyDerivationInfo.keyLength,
      keyDerivationInfo.saltLength
    )
    header ++= keyDerivationInfo.salt
  }

  private def writeHeaderToStream(): Unit = {
    outputStream.write(header)
    outputStream.flush()
    position += header.length
  }

  private def setupKeyInfo(): Unit = {
    keyInfo = new KeyInfo(key)
    keyInfo.iv = encryptionInfo.iv
    encryptionBoundary = position
  }

  protected def writeHeader(): Unit = {
    writeKeyDerivationInfo()
    writeEncryptionInfo()
    writeHeaderToStream()
    setupKeyInfo()
  }

  protected def writeHmac(): Unit = {
    write(BytesWrapper(CryptoUtils.hmac(header, keyInfo)))
  }

}
