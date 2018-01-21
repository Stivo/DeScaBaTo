package ch.descabato

import java.io.{File, FileOutputStream}
import java.security.SecureRandom

import ch.descabato.core.util._
import ch.descabato.utils.BytesWrapper
import org.scalacheck.Gen
import org.scalatest.{BeforeAndAfterAll, FlatSpec}
import org.scalatest.prop.GeneratorDrivenPropertyChecks

class EncryptionIOSpec extends FlatSpec with GeneratorDrivenPropertyChecks with BeforeAndAfterAll {

  val file = File.createTempFile("desca_original", "test")
  val encrypted = File.createTempFile("desca_encrypted", "test")
  val bytes: Array[Byte] = Array.ofDim[Byte](100000)
  "io" should "decrypt and encrypt correctly" in {
    setupRandomData()
    val key = BytesWrapper(bytes, 0, 16).asArray()
    val writer = new EncryptedFileWriter(encrypted, key)
    val startPos = writer.write(BytesWrapper(bytes))
    writer.finish()
    val reader = new EncryptedFileReader(encrypted, key)

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
