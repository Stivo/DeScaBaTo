package ch.descabato.web

import java.util.Date

import ch.descabato.core.FileDescription

import scalafx.scene.control._
import scalafxml.core.macros.{nested, sfxml}
import scalafx.Includes._
import scalafx.collections.ObservableBuffer

@sfxml
class RestoreController(
                         private val tabs: TabPane,
                         @nested[BrowserController] var browserController: ChildController,
                         @nested[PreviewController] var previewController: PreviewControllerI,
                         private val versionChooser: ComboBox[Date]
                       ) extends RestoreControllerI {
  browserController.registerMain(this)
  previewController.registerMain(this)

  versionChooser.items = ObservableBuffer(BackupViewModel.index.versions)
  versionChooser.selectionModel().selectLast()

  versionChooser.value.onChange { (_, _, newDate) =>
    println(s"New date selected ${newDate}")
    BackupViewModel.index.loadBackup(newDate)
    browserController.indexUpdated()
  }

  override def preview(fileDescription: FileDescription): Unit = {
    tabs.selectionModel().selectLast()
    previewController.preview(fileDescription)
  }
}

trait RestoreControllerI {
  def preview(fileDescription: FileDescription): Unit
}