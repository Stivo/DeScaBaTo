package ch.descabato.web

import java.io.IOException

import ch.descabato.core.{BackupConfigurationHandler, Universes}
import ch.descabato.frontend.RestoreConf

import scalafx.Includes._
import scalafx.application.{JFXApp, Platform}
import scalafx.application.JFXApp.PrimaryStage
import scalafx.scene.Scene
import scalafxml.core.{FXMLView, NoDependencyResolver}


/** Example of using FXMLLoader from ScalaFX.
  *
  * @author Jarek Sacha
  */
object ScalaFxGui extends JFXApp {

  val resource = getClass.getResource("welcome.fxml")
  if (resource == null) {
    throw new IOException("Cannot load resource: welcome.fxml")
  }

  val root = FXMLView(resource, NoDependencyResolver)

  stage = new PrimaryStage() {
    title = "DeScaBaTo Backup Browser"
    scene = new Scene(root)
  }

  def openRestore(folder: String): Unit = {
    new Thread() {
      override def run(): Unit = {

        val conf = new RestoreConf(Seq("--restore-to-original-path", folder))
        conf.verify()

        val handler = new BackupConfigurationHandler(conf)
        val config = handler.configure(None)
        val universe = Universes.makeUniverse(config)

        val index = new Index(universe)
        BackupViewModel.index = index
        WebServer.index = index
        val thread = new Thread(() => WebServer.main(Array.empty))
        thread.setDaemon(true)
        thread.start()

        val resource = getClass.getResource("restoreGui.fxml")
        if (resource == null) {
          throw new IOException("Cannot load resource: restoreGui.fxml")
        }

        Platform.runLater{
          val root = FXMLView(resource, NoDependencyResolver)
          stage.hide()
          stage.delegate.setScene(new Scene(root))
          stage.show()
        }
      }
    }.start()
  }

}