package ch.descabato.core.util

import java.time.LocalDateTime
import scala.collection.mutable


case class GrandFatheringConfig(latest: Int = 10, hourly: Int = 24, daily: Int = 7, weekly: Int = 4, monthly: Int = 12, yearly: Int = 5) {

  def config(): mutable.Map[String, (LocalDateTime => LocalDateTime, LocalDateTime => LocalDateTime, Int)] = mutable.LinkedHashMap(
    "latest" -> ( {
      _.minusSeconds(1)
    }, {
      _.minusYears(100)
    }, latest),
    "hourly" -> ( {
      _.minusHours(1).plusMinutes(1)
    }, {
      _.minusHours(hourly)
    }, hourly),
    "daily" -> ( {
      _.minusDays(1).plusMinutes(10)
    }, {
      _.minusDays(daily)
    }, daily),
    "weekly" -> ( {
      _.minusDays(7).plusHours(1)
    }, {
      _.minusDays(weekly * 7)
    }, weekly),
    "monthly" -> ( {
      _.minusMonths(1).plusHours(1)
    }, {
      _.minusMonths(monthly)
    }, monthly),
    "yearly" -> ( {
      _.minusYears(1).plusHours(1)
    }, {
      _.minusYears(yearly).minusHours(1)
    }, yearly),
  )
}
