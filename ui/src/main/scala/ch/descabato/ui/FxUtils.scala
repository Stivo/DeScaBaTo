package ch.descabato.ui

import java.io.File
import java.net.URL

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

  def getResource(resource: String): URL = {
    var url = _getResource(resource)
    if (url == null) {
      url = _getResource("/ch/descabato/ui/"+resource)
    }
    if (url == null) {
      url = _getResource("./"+resource)
    }
    if (url == null) {
      url = new File("ui/src/main/resources/ch/descabato/ui", resource).toURI.toURL
    }
    return url;
  }

  private def _getResource(resource: String): URL = {

    var url: URL = null;

    //Try with the Thread Context Loader.
    var classLoader = Thread.currentThread().getContextClassLoader()
    if(classLoader != null){
      url = classLoader.getResource(resource)
      if(url != null){
        return url
      }
    }

    //Let's now try with the classloader that loaded this class.
    classLoader = this.getClass.getClassLoader()
    if(classLoader != null){
      url = classLoader.getResource(resource)
      if(url != null){
        return url
      }
    }

    //Last ditch attempt. Get the resource from the classpath.
    return ClassLoader.getSystemResource(resource)
  }

}
