package ch.descabato.core.util

import java.io.{File, RandomAccessFile}
import java.util
import javax.crypto.Cipher

import ch.descabato.core_old.kvstore.{EncryptionInfo, KeyInfo}
import ch.descabato.utils.BytesWrapper

class EncryptedFileReader(val file: File, key: Array[Byte]) extends CipherUser(key) with FileReader {

  private var position = 0L

  private val raf: RandomAccessFile = new RandomAccessFile(file, "r")

  def readHeader(): Unit = {
    val bytes = Array.ofDim[Byte](magicMarker.length)
    raf.readFully(bytes)
    if (!util.Arrays.equals(bytes, magicMarker)) {
      throw new IllegalStateException(s"This is not an encrypted file $file")
    }
    val version = raf.read()
    if (version != 0) {
      throw new IllegalStateException(s"Unknown version $version")
    }
    val kvStoreType = raf.read()
    if (kvStoreType != 1) {
      throw new IllegalStateException(s"TODO: Implement $kvStoreType")
    }
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
    // TODO verify hMac
  }

  readHeader()

  override def readAllContent(): BytesWrapper = {
    val startOfContent = encryptionBoundary + 32
    val out = readChunk(startOfContent, file.length() - startOfContent)
    println(s"Read this ${new String(out.asArray())}")
    out
  }

  override def readChunk(position: Long, length: Long): BytesWrapper = {
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

