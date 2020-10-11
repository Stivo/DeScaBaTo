package ch.descabato

import java.util.Base64

import ch.descabato.utils.Implicits._
import ch.descabato.utils.FastHashMap
import ch.descabato.utils.Hash
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

class ByteArrayMapSpec extends AnyFlatSpec with ScalaCheckDrivenPropertyChecks {

  "ByteArrayMap" should "work with hashes and byte arrays as keys" in {
    var strings = Set.empty[String]
    var wrappers = new FastHashMap[Boolean]()
    val encoder = Base64.getEncoder()
    forAll(minSize(0), sizeRange(100), minSuccessful(10000)) { (toEncode: Array[Byte]) =>
      // reference: base 64 strings
      val string = encoder.encodeToString(toEncode)
      val contained = strings safeContains string
      // check that the byte arrays say the same
      assert(contained === (wrappers safeContains Hash(toEncode)))
      // add them to the set and map
      strings += string
      wrappers += Hash(toEncode) -> true
    }
    for (wrapper <- wrappers.keys) {
      assert(wrappers safeContains (wrapper))
      assert(wrappers safeContains (Hash(wrapper)))
    }
  }

}