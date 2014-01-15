package ch.descabato

trait ProgressReporting {

  def ephemeralMessage(message: String)

  def setTotalPercent(x: Int)

}

object ProgressReporter extends ProgressReporting {
  def ephemeralMessage(message: String) {}
  def setTotalPercent(x: Int) {}
}

class Windows7Reporting() extends ProgressReporting {

  def setTotalPercent(x: Int) {

  }

  def ephemeralMessage(message: String) {

  }
}