package ch.descabato

import java.io.File
import java.io.FileInputStream
import java.io.FileOutputStream
import java.security.SecureRandom

import ch.descabato.core.util._
import ch.descabato.utils.BytesWrapper
import ch.descabato.utils.Hash
import org.apache.commons.codec.digest.DigestUtils
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

import scala.util.Random

class FileWriterHashSpec extends AnyFlatSpec with ScalaCheckDrivenPropertyChecks with BeforeAndAfterAll {

  val file = File.createTempFile("desca_original", "test")
  val encrypted = File.createTempFile("desca_encrypted", "test")
  val bytes: Array[Byte] = Array.ofDim[Byte](1024 * 1024)
  private val random = new Random()
  private val password: String = random.nextString(16)
  println(s"password is $password")

  "sequential writer" should "compute correct hash" in {
    setupRandomData()
    val writer = new SimpleFileWriter(encrypted)
    testWriter(writer, true)
  }

  "encrypted writer" should "compute correct hash" in {
    setupRandomData()
    val writer = new EncryptedFileWriter(encrypted, password, 128)
    testWriter(writer)
  }

  private def testWriter(writer: FileWriter, inputShouldBeSameAsFileContent: Boolean = false): Unit = {
    val startPos = writer.write(BytesWrapper(bytes, 0, 213))
    var written = 213
    while (written < bytes.length) {
      val currentLength = random.nextInt(bytes.length - written + 1)
      writer.write(BytesWrapper(bytes, written, currentLength))
      written += currentLength
    }
    writer.close()
    val computedHashByWriter = writer.md5Hash()
    val fis = new FileInputStream(writer.file)
    val fromFile = Hash(DigestUtils.md5(fis))
    fis.close()
    assert(fromFile === computedHashByWriter)
    if (inputShouldBeSameAsFileContent) {
      assert(computedHashByWriter === Hash(DigestUtils.md5(bytes)))
    }
  }

  override protected def afterAll(): Unit = {
    file.delete()
    encrypted.delete()
  }

  private def setupRandomData() = {
    new SecureRandom().nextBytes(bytes)
    val stream = new FileOutputStream(file)
    stream.write(bytes)
    stream.close()
  }
}
