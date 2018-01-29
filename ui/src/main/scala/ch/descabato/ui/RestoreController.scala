package ch.descabato.ui

import java.util.Date

import ch.descabato.core.FileDescription

import scalafx.scene.control._
import scalafxml.core.macros.{nested, sfxml}
import scalafx.Includes._
import scalafx.application.Platform
import scalafx.collections.ObservableBuffer
import scalafx.scene.layout.BorderPane

@sfxml
class RestoreController(
                         private val togglePreview: ToggleButton,
                         @nested[BrowserController] var browserController: ChildController,
                         @nested[PreviewController] var previewController: PreviewControllerI,
                         private val versionChooser: ComboBox[Date],
                         private val previewPane: BorderPane,
                         private val splitPane: SplitPane
                       ) extends RestoreControllerI {
  browserController.registerMain(this)
  previewController.registerMain(this)

  versionChooser.items = ObservableBuffer(BackupViewModel.index.versions)
  versionChooser.selectionModel().selectLast()

  splitPane.items.remove(previewPane)

  togglePreview.selected.onChange { (_, _, newValue) =>
    if (newValue) {
      if (!splitPane.items.contains(previewPane)) {
        splitPane.items.add(previewPane)
      }
    } else {
      splitPane.items.remove(previewPane)
    }
  }

  versionChooser.value.onChange { (_, _, newDate) =>
    println(s"New date selected ${newDate}")
    BackupViewModel.index.loadBackup(newDate)
    browserController.indexUpdated()
  }

  override def preview(fileDescription: FileDescription): Unit = {
    togglePreview.selected = true
    previewController.preview(fileDescription)
  }

  override def previewIfOpen(fd: FileDescription): Unit = {
    if (togglePreview.selected()) {
      previewController.preview(fd)
    }
  }

  def close(): Unit = {
    Platform.exit()
  }

}

trait RestoreControllerI {
  def previewIfOpen(fd: FileDescription): Unit

  def preview(fileDescription: FileDescription): Unit
}