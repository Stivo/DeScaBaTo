package ch.descabato

import org.scalatest._
import java.io.File
import org.scalatest.Matchers._
import java.util.Arrays
import scala.collection.mutable.HashMap
import scala.collection.mutable.Buffer
import scala.collection.mutable.Set
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
import ch.descabato.utils.Utils
import ch.descabato.core.Size

class UtilSpec extends FlatSpec with BeforeAndAfterAll with GeneratorDrivenPropertyChecks with TestUtils {
  import org.scalacheck.Gen._
  
  //ConsoleManager.testSetup
  
  var folder = new File("testdata/temp")
  
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
    val size: Size = Size(s)
    assert(size.bytes === l)
  }
  
}
