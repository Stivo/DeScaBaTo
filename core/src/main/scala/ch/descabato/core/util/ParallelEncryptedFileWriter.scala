package ch.descabato.core.util

import java.io.File
import javax.crypto.Cipher

import ch.descabato.utils.BytesWrapper

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

class ParallelEncryptedFileWriter(file: File, passphrase: String, keylength: Int) extends EncryptedFileWriterBase(file, passphrase, keylength) {

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
      val (_, bytes) = Await.result(doneFuture, 1.second)
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

  writeHeader()
  startEncryptedPart()

  private def startEncryptedPart(): Unit = {
    writeHmac()
  }

  override def finishImpl(): Unit = {
    addFuture(position - leftOver.length, leftOver)
    while (!futures.isEmpty) {
      writeCompletedFutures()
      Thread.sleep(10)
    }

    outputStream.close()
  }

}
