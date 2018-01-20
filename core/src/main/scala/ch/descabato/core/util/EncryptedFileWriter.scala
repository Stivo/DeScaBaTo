//package ch.descabato.core.util
//
//import java.io.{File, FileOutputStream, OutputStream}
//import javax.crypto.{Cipher, CipherOutputStream}
//
//import ch.descabato.utils.Implicits._
//import akka.util.ByteString
//import ch.descabato.utils.BytesWrapper
//
//class EncryptedFileWriter(val file: File, key: ByteString) extends CipherUser(key) with FileWriter {
//
//  private var position = 0L
//
//  private var outputStream: OutputStream = new FileOutputStream(file)
//
//  var header = BytesWrapper.empty
//  header ++= magicMarker
//  header ++= kvStoreVersion
//  header :+= 1.toByte // type 1 continues with encryption parameters
//
//  private def writeEncryptionInfo() {
//    header :+= encryptionInfo.algorithm
//    header :+= encryptionInfo.macAlgorithm
//    header :+= encryptionInfo.ivLength
//    header ++= encryptionInfo.iv
//  }
//
//  def write(bytes: BytesWrapper): Long = {
//    val out = position
//    outputStream.write(bytes)
//    position += bytes.length
//    out
//  }
//
//
//  private def startEncryptedPart(key: BytesWrapper): Unit = {
//    outputStream.write(header)
//    position += header.length
//    keyInfo = new KeyInfo(key)
//    keyInfo.iv = encryptionInfo.iv
//    encryptionBoundary = position
//    initializeCipher(Cipher.ENCRYPT_MODE, keyInfo)
//    outputStream = new CipherOutputStream(outputStream, cipher)
//    write(CryptoUtils.hmac(header.toArray, keyInfo))
//  }
//
//  writeEncryptionInfo()
//  startEncryptedPart(key)
//
//  override def finish(): Unit = {
//    outputStream.close()
//  }
//}
//
//class EncryptionInfo(
//                      // algorithm 0 is for AES/CTR/NoPadding
//                      val algorithm: Byte = 0,
//                      // algorithm 0 is for HmacSHA256
//                      val macAlgorithm: Byte = 0,
//                      val ivLength: Byte = 16,
//                      val iv: ByteString = ByteString(CryptoUtils.newStrongRandomByteArray(16))
//                    )
