package ch.descabato.core.util

import java.io.File
import java.io.RandomAccessFile
import java.util

import ch.descabato.core.PasswordWrongException
import ch.descabato.utils.BytesWrapper
import javax.crypto.Cipher

class EncryptedFileReader(val file: File, passphrase: String) extends CipherUser(passphrase) with FileReader {

  private val raf: RandomAccessFile = new RandomAccessFile(file, "r")

  var key: Array[Byte] = null

  def readKeyDerivationInfo() = {
    val algorithm = raf.readByte()
    if (algorithm != 0) {
      throw new IllegalArgumentException(s"algorihtm $algorithm is not implemented")
    }
    val iterationsPower = raf.readByte()
    val memoryFactor = raf.readByte()
    val keyLength = raf.readByte()
    val saltLength = raf.readByte()
    val salt = Array.ofDim[Byte](saltLength)
    raf.readFully(salt)
    key = CryptoUtils.keyDerive(passphrase, salt, keyLength, iterationsPower, memoryFactor)
  }

  def readHeader(): Unit = {
    readGeneralHeader()
    val kvStoreType = raf.read()
    if (kvStoreType != 2) {
      throw new IllegalStateException(s"TODO: Implement $kvStoreType")
    }
    readKeyDerivationInfo()
    val algorithm = raf.read()
    if (algorithm != encryptionInfo.algorithm) {
      throw new IllegalStateException(s"TODO: Implement $algorithm")
    }
    val macAlgorithm = raf.read()
    if (macAlgorithm != encryptionInfo.algorithm) {
      throw new IllegalStateException(s"TODO: Implement $macAlgorithm")
    }
    val ivLength = raf.read()
    val iv = Array.ofDim[Byte](ivLength)
    raf.readFully(iv)
    encryptionInfo = new EncryptionInfo(algorithm.toByte, macAlgorithm.toByte, ivLength.toByte, iv)
    encryptionBoundary = raf.getFilePointer
    keyInfo = new KeyInfo(key)
    keyInfo.iv = encryptionInfo.iv
    initializeCipher(Cipher.DECRYPT_MODE, keyInfo)
    val hmacInFile = readChunk(encryptionBoundary, 32)
    raf.seek(0)
    val header = Array.ofDim[Byte](encryptionBoundary.toInt)
    raf.readFully(header)
    val hmacComputed = CryptoUtils.hmac(header, keyInfo)
    if (!util.Arrays.equals(hmacInFile.asArray(), hmacComputed)) {
      throw new PasswordWrongException("Hmac verification failed, either data is corrupt or password is wrong", null)
    }
  }

  private def readGeneralHeader() = {
    val bytes = Array.ofDim[Byte](magicMarker.length)
    raf.readFully(bytes)
    if (!util.Arrays.equals(bytes, magicMarker)) {
      throw new IllegalStateException(s"This is not an encrypted file $file")
    }
    val version = raf.read()
    if (version != 0) {
      throw new IllegalStateException(s"Unknown version $version")
    }
  }

  readHeader()

  override def readAllContent(): BytesWrapper = {
    val startOfContent = encryptionBoundary + 32
    val out = readChunk(startOfContent, file.length() - startOfContent)
    out
  }

  override def readChunk(position: Long, length: Long): BytesWrapper = {
    require(position >= encryptionBoundary)
    if (length == 0) {
      BytesWrapper.apply(Array.ofDim[Byte](0))
    } else {
      // compute offset to read from disk (bytes should be within)
      // round down
      val diskMin = ((position - encryptionBoundary) / 32) * 32 + encryptionBoundary
      // round up
      var diskEnd = (((position + length - encryptionBoundary) / 32) + 1) * 32 + encryptionBoundary
      // but do not overshoot over end of file ;)
      if (diskEnd > file.length()) {
        diskEnd = file.length()
      }
      raf.seek(diskMin)
      val value = Array.ofDim[Byte]((diskEnd - diskMin).toInt)
      raf.readFully(value)
      // decipher the blocks
      initializeCipher(Cipher.DECRYPT_MODE, keyInfo, ((diskMin - encryptionBoundary) / cipher.getBlockSize ).toInt)
      val deciphered = cipher.doFinal(value)
      // cut out the relevant part
      BytesWrapper(deciphered, (position - diskMin).toInt, length.toInt)
    }
  }

  override def close(): Unit = {
    raf.close()
  }
}

