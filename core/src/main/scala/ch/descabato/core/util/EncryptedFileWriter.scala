package ch.descabato.core.util

import java.io.{File, FileOutputStream, OutputStream}
import javax.crypto.Cipher

import akka.util.ByteString
import ch.descabato.akka.ActorStats.ex
import ch.descabato.core_old.kvstore.{CryptoUtils, KeyInfo}
import ch.descabato.utils.{BytesWrapper, Utils}

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

class EncryptedFileWriter(val file: File, val passphrase: String) extends CipherUser(passphrase) with FileWriter with Utils {

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

  private var futures: Seq[Future[(Long, Array[Byte])]] = Seq.empty

  private var leftOver: Array[Byte] = Array.emptyByteArray

  def write(bytes: BytesWrapper): Long = {
    val out = position
    if (leftOver.length + bytes.length > 32) {
      val startOfBlock = out - leftOver.length
      val todoEncrypt: Array[Byte] = leftOver ++ bytes.asArray()
      val multipleOfBlockLength = (todoEncrypt.length / 32) * 32
      val toEncrypt = todoEncrypt.slice(0, multipleOfBlockLength)
      leftOver = todoEncrypt.slice(multipleOfBlockLength, todoEncrypt.length)
      addFuture(startOfBlock, toEncrypt)
    } else {
      leftOver ++= bytes.asArray()
    }
    position += bytes.length
    writeCompletedFutures()
    while (futures.length > 10) {
      Thread.sleep(10)
      writeCompletedFutures()
    }
    out
  }

  private def writeCompletedFutures() = {
    while (!futures.isEmpty && futures.head.isCompleted) {
      val doneFuture = futures.head
      val (position, bytes) = Await.result(doneFuture, 1.second)
      outputStream.write(bytes)
      futures = futures.tail
    }
  }

  private def addFuture(startOfBlock: Long, toEncrypt: Array[Byte]) = {
    futures :+= Future {
      require((startOfBlock - encryptionBoundary) % 32 == 0)
      val iv = CryptoUtils.deriveIv(keyInfo.iv, ((startOfBlock - encryptionBoundary) / 16).toInt)
      val cipherHere = Cipher.getInstance("AES/CTR/NoPadding", "BC")
      cipherHere.init(Cipher.ENCRYPT_MODE, CryptoUtils.keySpec(keyInfo), iv)
      (startOfBlock, cipherHere.doFinal(toEncrypt))
    }
  }

  private def startEncryptedPart(key: Array[Byte]): Unit = {
    outputStream.write(header.toArray)
    outputStream.flush()
    position += header.length
    keyInfo = new KeyInfo(key)
    keyInfo.iv = encryptionInfo.iv
    encryptionBoundary = position
    write(BytesWrapper(CryptoUtils.hmac(header.toArray, keyInfo)))
  }

  writeKeyDerivationInfo()
  val keyHere = CryptoUtils.keyDerive(passphrase, keyDerivationInfo.salt,
    keyDerivationInfo.keyLength, keyDerivationInfo.iterationsPower, keyDerivationInfo.memoryFactor)
  writeEncryptionInfo()
  startEncryptedPart(keyHere)

  override def finish(): Unit = {
    addFuture(position - leftOver.length, leftOver)
    while (!futures.isEmpty) {
      writeCompletedFutures()
      Thread.sleep(10)
    }

    outputStream.close()
  }
}
