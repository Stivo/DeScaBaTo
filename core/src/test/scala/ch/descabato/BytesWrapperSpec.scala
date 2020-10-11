package ch.descabato

import ch.descabato.utils.BytesWrapper
import ch.descabato.utils.Implicits._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

class BytesWrapperSpec extends AnyFlatSpec with ScalaCheckDrivenPropertyChecks {

  "byteswrapper" should "implement equality" in {
    val b1 = "asdf".getBytes().wrap()
    val b2 = BytesWrapper("basdf".getBytes(), 1)
    assert(b1 === b2)
  }
  "byteswrapper" should "implement hashcode" in {
    val b1 = "asdf".getBytes().wrap()
    val b2 = BytesWrapper("basdf".getBytes(), 1)
    assert(b1.hashCode === b2.hashCode)
  }

}

