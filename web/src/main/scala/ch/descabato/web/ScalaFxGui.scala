package ch.descabato.web

import java.io.IOException

import scalafx.Includes._
import scalafx.application.JFXApp
import scalafx.application.JFXApp.PrimaryStage
import scalafx.scene.Scene
import scalafxml.core.{FXMLView, NoDependencyResolver}


/** Example of using FXMLLoader from ScalaFX.
  *
  * @author Jarek Sacha
  */
object ScalaFxGui extends JFXApp {

  val resource = getClass.getResource("restoreGui.fxml")
  if (resource == null) {
    throw new IOException("Cannot load resource: restoreGui.fxml")
  }

  val root = FXMLView(resource, NoDependencyResolver)

  stage = new PrimaryStage() {
    title = "DeScaBaTo Backup Browser"
    scene = new Scene(root)
  }

}