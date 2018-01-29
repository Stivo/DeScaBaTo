package ch.descabato.ui



import scalafx.Includes._
import scalafx.scene.control._
import scalafx.stage.DirectoryChooser
import scalafxml.core.macros.sfxml

@sfxml
class WelcomeController(
                         private val folderTextfield: TextField,
                         private val chooseFolderButton: Button,
                         private val openBackupButton: Button,
                         private val prefixTextField: TextField,
                         private val passwordTextfield: PasswordField
                       ) {

  if (System.getProperty("user.name") == "Stivo") {
    folderTextfield.text = "L:/backup"
    open()
  }

  def onChooseFolderButton(): Unit = {
    val file = new DirectoryChooser().showDialog(chooseFolderButton.getScene.getWindow)
    if (file != null) {
      folderTextfield.text = file.getCanonicalFile.getAbsolutePath
    }
  }

  def open(): Unit = {
    openBackupButton.text = "Loading ..."
    openBackupButton.disable = true
    ScalaFxGui.openRestore(folderTextfield.text())
  }

}

