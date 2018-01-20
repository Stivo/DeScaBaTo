//package ch.descabato.core.util
//
//import java.io.{File, RandomAccessFile}
//import java.util
//import javax.crypto.Cipher
//
//import akka.util.ByteString
//
//class EncryptedFileReader(val file: File, key: ByteString) extends CipherUser(key) with FileReader {
//
//  private var position = 0L
//
//  private val raf: RandomAccessFile = new RandomAccessFile(file, "r")
//
//  def readHeader(): Unit = {
//    val bytes = Array.ofDim[Byte](magicMarker.length)
//    raf.readFully(bytes)
//    if (!util.Arrays.equals(bytes, magicMarker)) {
//      throw new IllegalStateException(s"This is not an encrypted file $file")
//    }
//    val version = raf.read()
//    if (version != 0) {
//      throw new IllegalStateException(s"Unknown version $version")
//    }
//    val kvStoreType = raf.read()
//    if (kvStoreType != 1) {
//      throw new IllegalStateException(s"TODO: Implement $kvStoreType")
//    }
//    val algorithm = raf.read()
//    if (algorithm != encryptionInfo.algorithm) {
//      throw new IllegalStateException(s"TODO: Implement $algorithm")
//    }
//    val macAlgorithm = raf.read()
//    if (macAlgorithm != encryptionInfo.algorithm) {
//      throw new IllegalStateException(s"TODO: Implement $macAlgorithm")
//    }
//    val ivLength = raf.read()
//    val iv = Array.ofDim[Byte](ivLength)
//    raf.readFully(iv)
//    encryptionInfo = new EncryptionInfo(algorithm.toByte, macAlgorithm.toByte, ivLength.toByte, ByteString(iv))
//    encryptionBoundary = raf.getFilePointer
//    keyInfo = new KeyInfo(key)
//    keyInfo.iv = encryptionInfo.iv
//    initializeCipher(Cipher.DECRYPT_MODE, keyInfo)
//    // TODO verify hMac
//  }
//
//  readHeader()
//
//  override def readAllContent(): ByteString = {
//    val startOfContent = encryptionBoundary + 32
//    readChunk(startOfContent, file.length() - startOfContent)
//  }
//
//  def roundUp(toRound: Long, toRoundTo: Int) = {
//    val mod = toRound % toRoundTo
//    if (mod == 0) {
//      toRound
//    } else {
//      toRound + toRoundTo - mod
//    }
//  }
//
//  override def readChunk(position: Long, length: Long): ByteString = {
//    val encryptedOffset = position - encryptionBoundary
//    // compute offset to read from disk (bytes should be within)
//    val min = roundUp(encryptedOffset, 32) - 32
//    // compute offset to read to on disk (bytes should be within)
//    // read those blocks
//    val minDiskPosition = min + encryptionBoundary
//    raf.seek(minDiskPosition)
//    val lengthToRead = Math.min(32 + roundUp(length, 32), file.length() - minDiskPosition)
//    val value = Array.ofDim[Byte](lengthToRead.toInt)
//    raf.readFully(value)
//    // decipher the blocks
//    initializeCipher(Cipher.DECRYPT_MODE, keyInfo, min.toInt)
//    val deciphered = cipher.doFinal(value)
//    // cut out the relevant part
//    ByteString.fromArray(deciphered, (position - minDiskPosition).toInt, length.toInt)
//  }
//
//  override def close(): Unit = {
//    raf.close()
//  }
//}
//
