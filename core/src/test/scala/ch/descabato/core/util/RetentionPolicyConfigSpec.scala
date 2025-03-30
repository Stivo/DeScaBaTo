package ch.descabato.core.util

import org.scalatest.flatspec.AnyFlatSpec

import java.time.LocalDateTime
import scala.util.Random

class RetentionPolicyConfigSpec extends AnyFlatSpec {

  val end = LocalDateTime.now().withNano(0)

  val start = end.minusMonths(12)
  println(start)
  println(end)


  var list: Seq[LocalDateTime] = {
    var out = Seq.empty[LocalDateTime]
    var cur = start
    while (cur.isBefore(end) || cur == end) {
      cur = cur.plusHours(24)
      val x = Random.nextInt(120) - 60
      out :+= cur.plusSeconds(x)
    }
    out
  }


  //  println(GrandFatheringConfig(weekly = 20).applyTo(newList).mkString("\n"))
  //
  //  println(addTags(list, Duration.ofHours(1), "hours", 24))
  //  println(addTags(list, Duration.ofDays(1), "days", 31))
  //  println(addTags2(list, {x => x.minusDays(7)}, "weeks", 4))
  //  println(addTags2(list, {x => x.minusMonths(1)}, "months", 12))


}
