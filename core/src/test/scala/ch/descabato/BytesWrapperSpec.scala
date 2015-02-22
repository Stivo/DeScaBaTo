package ch.descabato

import ch.descabato.core._
import ch.descabato.utils.BytesWrapper
import ch.descabato.utils.Implicits._
import org.scalatest._

import scala.collection.mutable

class BytesWrapperSpec extends FlatSpec {

   "byteswrapper" should "implement equality" in {
    val b1 = "asdf".getBytes().wrap()
    val b2 = new BytesWrapper("basdf".getBytes(), 1)
    assert(b1 === b2)
  }

}