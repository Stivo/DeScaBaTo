package ch.descabato.rocks

import java.text.DecimalFormat


object Utils {

  def formatDuration(nanos: Long): String = {
    val millis = nanos / 1000 / 1000
    val seconds = millis / 1000
    if (seconds == 0) {
      f"${millis} ms"
    } else if (seconds < 10) {
      f"${millis * 0.001}%02.1f seconds"
    } else {
      val minutes = seconds / 60
      val hours = minutes / 60
      val days = hours / 24
      val add = if (days == 0) "" else s"$days days "
      f"$add${hours % 24}%02d:${minutes % 60}%02d:${seconds % 60}%02d"
    }
  }

  private val units = Array[String]("B", "KB", "MB", "GB", "TB")

  def readableFileSize(size: Long, afterDot: Int = 1): String = {
    if (size <= 0) return "0"
    val digitGroups = (Math.log10(size.toDouble) / Math.log10(1024)).toInt
    val afterDotPart = if (afterDot == 0) "#" else "0" * afterDot
    new DecimalFormat("#,##0. " + afterDotPart).format(size / Math.pow(1024, digitGroups)) + units(digitGroups)
  }

}

trait MeasureTime {
  var startTime = 0L

  def startMeasuring(): Unit = {
    startTime = System.nanoTime()
  }

  def measuredTime(): String = {
    val time = System.nanoTime()
    Utils.formatDuration(time - startTime)
  }

  def timeInMs(): Long = {
    val time = System.nanoTime()
    (time - startTime) / 1_000_000
  }

}

class StandardMeasureTime extends MeasureTime {
  startMeasuring()
}

object StopWatch {
  private def register(watch: StopWatch): Unit = {
    synchronized {
      stopwatches :+= watch
    }
  }

  private var stopwatches: List[StopWatch] = List.empty

  def report: String = stopwatches.map(_.format).mkString("\n")
}

class StopWatch(name: String) {
  StopWatch.register(this)
  private var accumulated = 0L
  private var invocations = 0L

  def measure[T](f: => T): T = {
    invocations += 1
    val start = System.nanoTime()
    val out = f
    accumulated += System.nanoTime() - start
    out
  }

  def format: String = {
    if (invocations > 0) {
      s"$name called ${invocations} times, took ${Utils.formatDuration(accumulated)}. ${accumulated / invocations / 1_000_000.0} ms / invocation"
    } else {
      s"$name was never called"
    }
  }
}