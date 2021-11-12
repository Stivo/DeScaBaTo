package ch.descabato

import java.io.File

import ch.descabato.core.model.Size
import org.scalatest._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

class UtilSpec extends AnyFlatSpec with BeforeAndAfterAll with ScalaCheckDrivenPropertyChecks with TestUtils {

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

  def test(s: String, l: Long): Unit = {
    val size: Size = Size(s)
    assert(size.bytes === l)
  }
  
}
