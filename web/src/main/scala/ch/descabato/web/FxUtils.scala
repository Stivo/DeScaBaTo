package ch.descabato.web

import scalafx.application.Platform

object FxUtils {

  def runInBackgroundThread(x: => Unit): Unit = {
    new Thread() {
      override def run(): Unit = {
        x
      }
    }.start()
  }

  def runInUiThread(x: => Unit): Unit = {
    Platform.runLater(x)
  }

}
