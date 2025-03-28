package ch.descabato.core.util

import org.scalatest.flatspec.AnyFlatSpec

import java.time.LocalDateTime
import java.time.ZoneOffset
import scala.collection.SortedMap
import scala.util.Random

class GrandfatheringSpec extends AnyFlatSpec {

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

  //  println(list)

  def addTags(list: Seq[LocalDateTime],
              calculateNext: LocalDateTime => LocalDateTime,
              minimumTimestampCalc: LocalDateTime => LocalDateTime,
              tagName: String, count: Int) = {
    val descending = list.toList.sortBy(x => -1 * x.toEpochSecond(ZoneOffset.UTC))
    var out = Seq.empty[(LocalDateTime, String)]
    var tagsGiven = 1
    var iterator = descending.iterator
    var lastTag = descending.head
    val minimumTimestamp = minimumTimestampCalc(lastTag)

    def tag(dateTime: LocalDateTime): Unit = {
      out :+= (dateTime, tagName + "-" + tagsGiven)
      tagsGiven += 1
      lastTag = dateTime
    }

    tag(descending.head)
    while (tagsGiven <= count && iterator.hasNext) {
      val mustBeBefore = calculateNext(lastTag)
      iterator = iterator.dropWhile(_.isAfter(mustBeBefore))
      if (iterator.hasNext) {
        val time = iterator.next()
        if (time.isBefore(minimumTimestamp)) {
          // end the loop
          tagsGiven = count + 1
        } else {
          tag(time)
        }
      }
    }
    out
  }

  def applyConfig(config: GrandFatheringConfig, timestamps: Seq[LocalDateTime]) = {
    var out = SortedMap.empty[LocalDateTime, Seq[String]]
    for ((tag, (period, minimumTimestampCalc, count)) <- config.config()) {
      addTags(list, period, minimumTimestampCalc, tag, count).foreach { case (dt, tag) =>
        out += dt -> (out.getOrElse(dt, Seq.empty) :+ tag)
      }
    }
    out
  }

  println(applyConfig(GrandFatheringConfig(weekly = 20), list).mkString("\n"))
  //
  //  println(addTags(list, Duration.ofHours(1), "hours", 24))
  //  println(addTags(list, Duration.ofDays(1), "days", 31))
  //  println(addTags2(list, {x => x.minusDays(7)}, "weeks", 4))
  //  println(addTags2(list, {x => x.minusMonths(1)}, "months", 12))


}
