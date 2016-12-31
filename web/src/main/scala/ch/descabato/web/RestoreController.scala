package ch.descabato.web

import ch.descabato.core.FileDescription

import scalafx.scene.control._
import scalafxml.core.macros.{nested, sfxml}

@sfxml
class RestoreController(
                         private val tabs: TabPane,
                         @nested[BrowserController] var browserController: ChildController,
                         @nested[PreviewController] var previewController: PreviewControllerI
                       ) extends RestoreControllerI {
  browserController.registerMain(this)
  previewController.registerMain(this)

  override def preview(fileDescription: FileDescription): Unit = {
    tabs.selectionModel().selectLast()
    previewController.preview(fileDescription)
  }
}

trait RestoreControllerI {
  def preview(fileDescription: FileDescription): Unit
}