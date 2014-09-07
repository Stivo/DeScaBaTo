package ch.descabato

import ch.descabato.core._
import org.scalatest._

import scala.collection.mutable

class CompressionDeciderSpec extends FlatSpec {
  import ch.descabato.core.StatisticHelper._

  val toTest = new SmartCompressionDecider()

  "computing median" should "work with longs" in {
    makeBuffer(1, 3, 5).median(17) === 3
    makeBuffer(5, 1, 3).median(17) === 3
    makeBuffer(1).median(17) === 1
    makeBuffer().median(17) === 17
  }

  "computing median" should "work with floats" in {
    makeBufferFloat(1.3, 3.8, 5).median(17) === 3.8
    makeBufferFloat(7, 2, 1.3, 3.8, 5).median(17) === 3.8
    makeBufferFloat(1).median(17) === 1
    makeBufferFloat().median(17) === 17
  }

  def makeBuffer(xs: Long*) = {
    val buf = mutable.Buffer[Long]()
    for (x <- xs)
      buf insertSorted x
    buf
  }

  def makeBufferFloat(xs: Double*) = {
    val buf = mutable.Buffer[Float]()
    for (x <- xs)
      buf insertSorted x.toFloat
    buf
  }

}
