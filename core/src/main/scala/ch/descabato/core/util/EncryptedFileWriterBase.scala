package ch.descabato.core.util

import java.io.File

import akka.util.ByteString
import ch.descabato.utils.{BytesWrapper, Utils}

abstract class EncryptedFileWriterBase(val file: File, val passphrase: String, keylength: Int) extends CipherUser(passphrase) with FileWriter with Utils{

  lazy val keyDerivationInfo = new KeyDerivationInfo(keyLength = (keylength / 8).toByte)

  protected val key = CryptoUtils.keyDerive(passphrase, keyDerivationInfo.salt,
    keyDerivationInfo.keyLength, keyDerivationInfo.iterationsPower, keyDerivationInfo.memoryFactor)

  private var header = ByteString.empty
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

  private def writeHeaderToStream() = {
    outputStream.write(header.toArray)
    outputStream.flush()
    position += header.length
  }

  private def setupKeyInfo() = {
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

  protected def writeHmac() {
    write(BytesWrapper(CryptoUtils.hmac(header.toArray, keyInfo)))
  }

}
