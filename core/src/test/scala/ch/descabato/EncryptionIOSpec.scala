package ch.descabato

import java.io.{File, FileOutputStream}
import java.security.SecureRandom

import ch.descabato.core.util._
import ch.descabato.utils.BytesWrapper
import org.scalacheck.Gen
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{BeforeAndAfterAll, FlatSpec}

import scala.util.Random

class EncryptionIOSpec extends FlatSpec with GeneratorDrivenPropertyChecks with BeforeAndAfterAll {

  val file = File.createTempFile("desca_original", "test")
  val encrypted = File.createTempFile("desca_encrypted", "test")
  val bytes: Array[Byte] = Array.ofDim[Byte](1024 * 1024)
  private val random = new Random()
  private val password: String = random.nextString(16)
  println(s"password is $password")

  "sequential writer" should "decrypt and encrypt correctly" in {
    setupRandomData()
    val writer = new EncryptedFileWriter(encrypted, password)
    testWriter(writer)
  }

  "parallel writer" should "decrypt and encrypt correctly" in {
    setupRandomData()
    val writer = new ParallelEncryptedFileWriter(encrypted, password)
    testWriter(writer)
  }

  private def testWriter(writer: FileWriter) = {
    val startPos = writer.write(BytesWrapper(bytes, 0, 213))
    var written = 213
    while (written < bytes.length) {
      val currentLength = random.nextInt(bytes.length - written + 1)
      writer.write(BytesWrapper(bytes, written, currentLength))
      written += currentLength
    }
    writer.finish()
    val reader = new EncryptedFileReader(encrypted, password)

    val myGen = for {
      n <- Gen.choose(0, bytes.length)
      m <- Gen.choose(0, bytes.length - n)
    } yield (n, m)

    forAll(myGen, minSuccessful(100)) { case (start, length) =>
      val wrapper = reader.readChunk(startPos + start, length)

      val bool = wrapper === BytesWrapper(bytes, start, length)
      if (!bool) {
        println(s"Failed with $start and $length")
      }
      assert(bool)
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
