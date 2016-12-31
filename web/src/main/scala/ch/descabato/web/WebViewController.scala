package ch.descabato.web

import ch.descabato.core.Size

import scalafx.Includes._
import scalafx.scene.control._
import scalafx.scene.web.WebView
import scalafxml.core.macros.sfxml

@sfxml
class WebViewController(
                         private val webview: WebView
                       ) {

  webview.engine.load("http://localhost:8080/preview.html")
}