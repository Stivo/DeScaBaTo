package ch.descabato.ui

import java.io.IOException

import ch.descabato.core.Universe
import ch.descabato.core.config.BackupConfigurationHandler
import ch.descabato.core.config.BackupVerification.{OK, VerificationResult}
import ch.descabato.frontend.RestoreConf
import ch.descabato.utils.Utils

import scala.concurrent.Await
import scala.concurrent.duration._
import scalafx.Includes._
import scalafx.application.JFXApp.PrimaryStage
import scalafx.application.{JFXApp, Platform}
import scalafx.scene.Scene
import scalafx.stage.WindowEvent
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

  def openRestore(folder: String, passphrase: String): VerificationResult = {
    var args = Seq("--restore-to-original-path", folder)
    if (passphrase != null && passphrase.nonEmpty) {
      args = "--passphrase" +: passphrase +: args
    }
    val conf = new RestoreConf(args)
    conf.verify()

    val handler = new BackupConfigurationHandler(conf, true)
    val result = handler.verify()

    if (result == OK) {
      val config = handler.updateAndGetConfiguration()
      new Thread() {
        override def run(): Unit = {

          val universe = new Universe(config)
          Await.result(universe.startup(), 1.minute)

          val index = new Index(universe)
          BackupViewModel.index = index

          openBrowser()
        }
      }.start()
    }
    result
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
      stage.setOnCloseRequest((event: WindowEvent) => {
        System.exit(0)
      })
    }
  }
}