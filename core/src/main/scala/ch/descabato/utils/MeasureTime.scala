package ch.descabato.utils


trait MeasureTime {
  var startTime = 0L

  def startMeasuring() {
    startTime = System.nanoTime()
  }

  def measuredTime(): String = {
    val time = System.nanoTime()
    format(time - startTime)
  }

  def format(nanos: Long): String = {
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
      val add = if (days == 0) "" else days + " days "
      f"$add${hours % 24}%02d:${minutes % 60}%02d:${seconds % 60}%02d"
    }
  }

}

class StandardMeasureTime extends MeasureTime {
  startMeasuring()
}
