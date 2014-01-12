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
import java.util.{List => JList}
import java.util.ArrayList
import scala.collection.convert.DecorateAsScala
import scala.collection.JavaConversions._
import java.io.ByteArrayInputStream
import java.io.InputStream
import org.scalatest.prop.GeneratorDrivenPropertyChecks

class UtilSpec extends FlatSpec with BeforeAndAfterAll with GeneratorDrivenPropertyChecks {

  ConsoleManager.testSetup
 
 "sizeparser" should "parse sizes" in {
    val mb = 1024*1024
    test("512B", 512)
    test("1MB", mb)
    test("1 MB", mb)
    test("102.4kB", (102.4*1024).toLong)
    test("1gb", 1024*mb)
    test("0.5gb", 512*mb)
  }
  
  def test(s: String, l: Long) {
    import Utils._
    val size : Size = s
    assert(size.bytes === l)
  }
 
  "aes encryption" should "work" in {
    testEncryption("password", "plaintext")
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
    (compare should equal (plaintext))
  }

  forAll { (password: String, plaintext: String) =>

//  whenever (d != 0 && d != Integer.MIN_VALUE
//      && n != Integer.MIN_VALUE) {

    testEncryption(password, plaintext)

  }
}
