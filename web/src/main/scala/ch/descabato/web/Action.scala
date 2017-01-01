package ch.descabato.web

import java.io.File

import scalafx.scene.control.Alert.AlertType
import scalafx.scene.control.{Alert, ButtonType, MenuItem}

trait ActionFactory[T] {
  def createAction(selection: SelectedItems, controller: T): Action[T]
}

abstract class Action[T](val selection: SelectedItems, val controller: T) {

  def isEnabled: Boolean

  def execute(): Unit

  def title: String

  def asMenuItem(): MenuItem = {
    new MenuItem() {
      text = title
      onAction = { _ =>
        execute()
      }
    }
  }
}

class PreviewActionFactory extends ActionFactory[BrowserController] {

  def createAction(selection: SelectedItems, browserController: BrowserController): Action[BrowserController] = {

    new Action(selection, browserController) {
      override val isEnabled = {
        selection match {
          case SelectedFile(_) => true
          case _ => false
        }
      }

      override def execute(): Unit = {
        selection match {
          case SelectedFile(fd) => browserController.preview()
        }
      }

      override val title: String = "Preview"
    }
  }

}

class ShowFilesFromSubfoldersActionFactory extends ActionFactory[BrowserController] {

  def createAction(selection: SelectedItems, browserController: BrowserController): Action[BrowserController] = {

    new Action(selection, browserController) {
      override val isEnabled = true

      override def execute(): Unit = {
        browserController.showSubfolderFiles.selectedProperty().set(!browserController.showSubfolderFiles.selectedProperty().get())
      }

      override val title: String = {
        if (browserController.showSubfolderFiles.selectedProperty().get()) {
          "Hide files from subfolders"
        } else {
          "Show files from subfolders"
        }
      }
    }
  }

}


class RestoreToOriginalPlaceActionFactory extends ActionFactory[BrowserController] {

  def createAction(selection: SelectedItems, browserController: BrowserController): Action[BrowserController] = {

    new Action(selection, browserController) {
      override val isEnabled = selection match {
        case NoneSelected => false
        case _ => true
      }

      override def execute(): Unit = {
        val file = new File(selection.items(0).path)
        if (file.exists()) {
          val alert = new Alert(AlertType.Confirmation)
          alert.title = "File already exists"
          alert.headerText = s"Do you want to overwrite ${file.getCanonicalPath}?"
          alert.contentText = "Press ok to overwrite, cancel to cancel restore"

          val result = alert.showAndWait()
          if (result != Some(ButtonType.OK)){
            return
          }
        }
        browserController.restoreToOriginal(selection.items)
      }

      override val title: String = "Restore to original place"
    }
  }

}