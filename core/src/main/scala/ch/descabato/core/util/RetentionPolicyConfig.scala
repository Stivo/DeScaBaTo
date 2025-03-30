package ch.descabato.core.util

import ch.descabato.protobuf.keys.RevisionValue

import java.time.LocalDateTime
import java.time.OffsetDateTime
import java.time.ZoneOffset
import scala.collection.SortedMap
import scala.collection.mutable


case class RetentionPolicyConfig(latest: Int = 10, hourly: Int = 24, daily: Int = 7, weekly: Int = 4, monthly: Int = 12, yearly: Int = 10) {

  def config: mutable.Map[String, (LocalDateTime => LocalDateTime, LocalDateTime => LocalDateTime, Int)] = mutable.LinkedHashMap(
    "latest" -> ((_.minusSeconds(1)), (_.minusYears(100)), latest),
    "hourly" -> ((_.minusHours(1).plusMinutes(1)), (_.minusHours(hourly)), hourly),
    "daily" -> ((_.minusDays(1).plusMinutes(10)), (_.minusDays(daily)), daily),
    "weekly" -> ((_.minusDays(7).plusHours(1)), (_.minusDays(weekly * 7)), weekly),
    "monthly" -> ((_.minusMonths(1).plusHours(1)), (_.minusMonths(monthly)), monthly),
    "yearly" -> ((_.minusYears(1).plusHours(1)), (_.minusYears(yearly).minusHours(1)), yearly),
  )

  def addTags(list: Iterable[LocalDateTime],
              calculateNext: LocalDateTime => LocalDateTime,
              minimumTimestampCalc: LocalDateTime => LocalDateTime,
              tagName: String, count: Int) = {
    var out = Seq.empty[(LocalDateTime, String)]
    if (count > 0) {
      val descending = list.toList.sortBy(x => -1 * x.toEpochSecond(ZoneOffset.UTC))
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
      val last = descending.last
      if (tagsGiven < count && minimumTimestamp.isBefore(last)) {
        tag(last)
      }
    }
    out
  }


  def applyTo(inputs: Seq[RevisionValue]): Map[RevisionValue, Seq[String]] = {
    val map = inputs.map { x => (LocalDateTime.ofEpochSecond(x.created / 1000, 0, OffsetDateTime.now().getOffset), x) }.toMap
    var out = SortedMap.empty[LocalDateTime, Seq[String]]
    for ((tag, (period, minimumTimestampCalc, count)) <- config) {
      addTags(map.keys, period, minimumTimestampCalc, tag, count).foreach { case (dt, tag) =>
        out += dt -> (out.getOrElse(dt, Seq.empty) :+ tag)
      }
    }
    out.toSeq.map { (timestamp, tags) =>
      map(timestamp) -> tags
    }.toMap
  }

}
