package backup

import org.scalatest._
import java.io.File
import org.scalatest.Matchers._
import java.util.Arrays
import scala.collection.mutable.HashMap
import scala.collection.mutable.Buffer
import scala.collection.mutable.Set
import java.io.ByteArrayOutputStream
import scala.collection.mutable.ArrayBuffer
import java.util.{ List => JList }
import java.util.ArrayList
import scala.collection.convert.DecorateAsScala
import scala.collection.JavaConversions._
import java.io.ByteArrayInputStream
import java.io.InputStream
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalacheck.Gen
import scala.util.Random
import org.scalacheck.Arbitrary
import org.scalacheck._
import Arbitrary.arbitrary

class UtilSpec extends FlatSpec with BeforeAndAfterAll with GeneratorDrivenPropertyChecks {
  import org.scalacheck.Gen._
  
  ConsoleManager.testSetup

  "sizeparser" should "parse sizes" in {
    val mb = 1024 * 1024
    test("512B", 512)
    test("1MB", mb)
    test("1 MB", mb)
    test("102.4kB", (102.4 * 1024).toLong)
    test("1gb", 1024 * mb)
    test("0.5gb", 512 * mb)
  }

  def test(s: String, l: Long) {
    import Utils._
    val size: Size = s
    assert(size.bytes === l)
  }
  
  implicit class InputStreamBetter(in: InputStream) {
    def readFully = {
      val baos = new ByteArrayOutputStream()
      Streams.copy(in, baos)
      baos.toByteArray()
    }
  }

  def randomElement[T](ar: Seq[T]) = {
    val r = new Random()
    val int = r.nextInt(ar.length - 1)
    ar(int)
  }

  implicit val genNode = Arbitrary {
    for {
      v <- oneOf(CompressionMode.values)
      keyLength <- oneOf(128, 192, 256)
    } yield {
      val out = new FHO();
      out.compression = v;
      out.keyLength = keyLength;
      out
    }
  }
  
  class FHO extends FileHandlingOptions {
    override def toString = s"Compression mode is $compression, keylength is $keyLength"
  }

  def testEncryption(password: String, plaintext: String) {
    var baos = new ByteArrayOutputStream()
    val out = AES.wrapStreamWithEncryption(baos, password, 128)
    out.write(plaintext.getBytes("UTF-8"))
    out.close()
    val ar = baos.toByteArray()
    baos.reset()
    val back = new ByteArrayInputStream(ar)
    val dec = AES.wrapStreamWithDecryption(back, password, 128)
    Streams.copy(dec, baos)
    val compare = new String(baos.toByteArray(), "UTF-8")
    (compare should equal(plaintext))
  }

  forAll { (fho: FHO, passphrase: Option[String], toEncode: Array[Byte]) =>
    passphrase.foreach(fho.passphrase = _)
    val baosOriginal = new ByteArrayOutputStream()
    val wrapped = StreamHeaders.wrapStream(baosOriginal, fho)
    wrapped.write(toEncode)
    wrapped.close()
    val encoded = baosOriginal.toByteArray()
    if (passphrase.isDefined && fho.compression == CompressionMode.none) {
      val baosVerify = new ByteArrayOutputStream()
      val wrapped = AES.wrapStreamWithEncryption(baosVerify, passphrase.get, fho.keyLength)
      wrapped.write(toEncode)
      wrapped.close()
      val verify = baosVerify.toByteArray()
      assert(Arrays.equals(encoded.drop(encoded.length - verify.length), verify))
    }
    val in = new ByteArrayInputStream(encoded)
    val read = StreamHeaders.readStream(in, passphrase).readFully
    assert(Arrays.equals(read, toEncode))
  }

  forAll { (password: String, plaintext: String) =>
    testEncryption(password, plaintext)
  }

}