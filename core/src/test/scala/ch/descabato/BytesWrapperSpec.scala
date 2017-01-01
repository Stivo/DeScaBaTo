package ch.descabato

import ch.descabato.utils.BytesWrapper
import ch.descabato.utils.Implicits._
import org.scalatest._
import org.scalatest.prop.GeneratorDrivenPropertyChecks

class BytesWrapperSpec extends FlatSpec with GeneratorDrivenPropertyChecks {

  "byteswrapper" should "implement equality" in {
    val b1 = "asdf".getBytes().wrap()
    val b2 = new BytesWrapper("basdf".getBytes(), 1)
    assert(b1 === b2)
  }
  "byteswrapper" should "implement hashcode" in {
    val b1 = "asdf".getBytes().wrap()
    val b2 = new BytesWrapper("basdf".getBytes(), 1)
    assert(b1.hashCode === b2.hashCode)
  }

}

