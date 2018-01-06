package ch.descabato.ui

import java.io.IOException

import ch.descabato.core.{BackupConfigurationHandler, Universes}
import ch.descabato.frontend.RestoreConf
import ch.descabato.utils.Utils

import scalafx.Includes._
import scalafx.application.{JFXApp, Platform}
import scalafx.application.JFXApp.PrimaryStage
import scalafx.scene.Scene
import scalafxml.core.{FXMLView, NoDependencyResolver}


object ScalaFxGui extends JFXApp with Utils {

  stage = new PrimaryStage() {
    title = "DeScaBaTo Backup Browser"
  }

  if (BackupViewModel.index == null) {
    openChooser()
  } else {
    openBrowser()
  }

  def openChooser() {
    val resource = getClass.getResource("/ch/descabato/ui/welcome.fxml")
    if (resource == null) {
      throw new IOException("Cannot load resource: welcome.fxml")
    }

    val root = FXMLView(resource, NoDependencyResolver)
    stage.scene = new Scene(root)
  }

  def openRestore(folder: String): Unit = {
    new Thread() {
      override def run(): Unit = {
        val conf = new RestoreConf(Seq("--restore-to-original-path", folder))
        conf.verify()

        val handler = new BackupConfigurationHandler(conf)
        val config = handler.configure(None)
        config.threads = 1

        val universe = Universes.makeUniverse(config)

        val index = new Index(universe)
        BackupViewModel.index = index

        openBrowser()
      }
    }.start()
  }

  private def openBrowser() = {
    val resource = getClass.getResource("/ch/descabato/ui/restoreGui.fxml")
    if (resource == null) {
      throw new IOException("Cannot load resource: restoreGui.fxml")
    }

    Platform.runLater {
      val root = FXMLView(resource, NoDependencyResolver)
      stage.hide()
      stage.delegate.setScene(new Scene(root))
      stage.show()
    }
  }
}