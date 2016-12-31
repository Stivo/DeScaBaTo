package ch.descabato.web

import java.net.URLEncoder

import ch.descabato.core.{FileDescription, Size}

import scalafx.Includes._
import scalafx.scene.control._
import scalafx.scene.web.WebView
import scalafxml.core.macros.sfxml

trait PreviewControllerI extends ChildController {
  def preview(fileDescription: FileDescription): Unit
}

@sfxml
class PreviewController(
                         val webview: WebView
                       ) extends PreviewControllerI {
  def preview(fileDescription: FileDescription) = {
    val path = fileDescription.path
    val encoded = URLEncoder.encode(path, "UTF-8")
    webview.engine.load("http://localhost:8080/preview.html?path="+encoded)
  }

}