package ch.descabato

import java.util.Base64

import ch.descabato.utils.{ByteArrayMap, BytesWrapper, Hash}
import org.scalatest.FlatSpec
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import ch.descabato.utils.Implicits._

class ByteArrayMapSpec extends FlatSpec with GeneratorDrivenPropertyChecks {

  "ByteArrayMap" should "work with hashes and byte arrays as keys" in {
    var strings = Set.empty[String]
    var wrappers = new ByteArrayMap[Boolean]()
    val encoder = Base64.getEncoder()
    forAll(minSize(0), sizeRange(100), minSuccessful(10000)) { (toEncode: Array[Byte]) =>
      val string = encoder.encodeToString(toEncode)
      val contained = strings safeContains string
      val hash = new Hash(toEncode)
      val containedwrapper = wrappers safeContains hash
      assert(contained === (wrappers safeContains new Hash(toEncode)))
      strings += string
      if (toEncode.length % 2 == 0) {
        wrappers += toEncode -> true
      }
    }
    for (wrapper <- wrappers.keys) {
      assert(wrappers safeContains (wrapper))
      assert(wrappers safeContains (new Hash(wrapper)))
    }
  }

}