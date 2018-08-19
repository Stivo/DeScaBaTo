package ch.descabato

import java.util.Base64

import ch.descabato.utils.Implicits._
import ch.descabato.utils.{ByteArrayMap, Hash}
import org.scalatest.FlatSpec
import org.scalatest.prop.GeneratorDrivenPropertyChecks

class ByteArrayMapSpec extends FlatSpec with GeneratorDrivenPropertyChecks {

  "ByteArrayMap" should "work with hashes and byte arrays as keys" in {
    var strings = Set.empty[String]
    var wrappers = new ByteArrayMap[Boolean]()
    val encoder = Base64.getEncoder()
    forAll(minSize(0), sizeRange(100), minSuccessful(10000)) { (toEncode: Array[Byte]) =>
      // reference: base 64 strings
      val string = encoder.encodeToString(toEncode)
      val contained = strings safeContains string
      // check that the byte arrays say the same
      assert(contained === (wrappers safeContains Hash(toEncode)))
      assert(contained === (wrappers safeContains toEncode))
      // add them to the set and map
      strings += string
      wrappers += toEncode -> true
    }
    for (wrapper <- wrappers.keys) {
      assert(wrappers safeContains (wrapper))
      assert(wrappers safeContains (Hash(wrapper)))
    }
  }

}