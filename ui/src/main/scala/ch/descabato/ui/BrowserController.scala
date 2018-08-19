package ch.descabato.ui


import javafx.collections.ObservableList

import ch.descabato.core.model.{BackupPart, FileDescription, FolderDescription, Size}

import scala.collection.mutable
import scalafx.Includes._
import scalafx.animation.PauseTransition
import scalafx.beans.binding.Bindings
import scalafx.beans.property.StringProperty
import scalafx.event.ActionEvent
import scalafx.scene.control._
import scalafx.scene.input.ContextMenuEvent
import scalafx.util.Duration
import scalafxml.core.macros.sfxml

sealed trait SelectedItems {
  def items: Seq[BackupPart]
}

case object NoneSelected extends SelectedItems {
  def items = List.empty
}
case class SelectedFile(fileDescription: FileDescription) extends SelectedItems {
  def items = Seq(fileDescription)
}
case class SelectedFiles(fileDescription: Seq[FileDescription]) extends SelectedItems {
  def items = fileDescription
}
case class SelectedFolder(folderDescription: BackupPart) extends SelectedItems {
  def items = Seq(folderDescription)
}
case class SelectedFolders(folderDescription: Seq[BackupPart]) extends SelectedItems {
  def items = folderDescription
}
case class SelectedBoth(folders: Seq[BackupPart], files: Seq[BackupPart]) extends SelectedItems {
  def items = folders ++ files
}

trait ChildController {
  protected var restoreControllerI: RestoreControllerI = null
  def registerMain(controller: RestoreControllerI) = restoreControllerI = controller
  def restoreController: RestoreControllerI = restoreControllerI

  def indexUpdated(): Unit = {}
}

@sfxml
class BrowserController(
                         private val browserTree: TreeView[BackupTreeNode],
                         private val browserTable: TableView[ObservableBackupPart],
                         private val browserSearch: TextField,
                         private val restoreButton: Button,
                         val showSubfolderFiles: CheckBox,
                         private val browserInfo: Label
                       ) extends ChildController {

  override def indexUpdated() = {
    selectionModelListenerSuspended = true
    model.indexUpdated()
    browserTree.root = model.root
    selectionModelListenerSuspended = false
  }

  val model = new BackupViewModel()
  browserTree.root = model.root
  browserTable.items = model.sortedItems

  def toggleShowSubfolderFiles(): Unit = {
    showSubfolderFiles.selected = !showSubfolderFiles.selected()
  }

  model.showSubfolders.bind(showSubfolderFiles.selected)

  browserTable.onSort = { _ => model.sortedItems.comparatorProperty().bind(browserTable.comparatorProperty()) }

  val items: ObservableList[ObservableBackupPart] = browserTable.selectionModel().getSelectedItems

  val searchText = new StringProperty("")

  val pause = new PauseTransition(Duration.apply(250))
  browserSearch.textProperty.onChange { (_, _, newValue) =>
    pause.onFinished = _ => searchText.value = newValue
    pause.playFromStart()
  }

  val selected = Bindings.createObjectBinding[SelectedItems]({ () =>
    println("Updating selection")
    if (items.isEmpty) {
      NoneSelected
    } else if (items.size() == 1) {
      if (items(0).backupPart.isFolder) {
        SelectedFolder(items(0).backupPart)
      } else {
        SelectedFile(items(0).backupPart.asInstanceOf[FileDescription])
      }
    } else {
      val (folders, files) = items.map(_.backupPart).partition(_.isFolder)
      if (folders.size == 0) {
        SelectedFiles(files.map(_.asInstanceOf[FileDescription]))
      } else if (files.size == 0) {
        SelectedFolders(folders)
      } else {
        SelectedBoth(folders.map(_.asInstanceOf[FolderDescription]), files.map(_.asInstanceOf[FileDescription]))
      }
    }
  }, items)

  updateInfo()

  model.filteredItems.onChange {
    (_, changes) => {
      updateInfo()
    }
  }
  items.onChange {
    (_, changes) => {
      updateInfo()
    }
  }
  model.shownFolder.onChange {
    (_, _, changes) => {
      updateInfo()
    }
  }

  selected.onChange {
    (_, _, newValue) => {
      newValue match {
        case SelectedFile(fd) =>
          restoreControllerI.previewIfOpen(fd)
        case _ =>
      }
    }
  }

  private def updateInfo() = {
    val itemsInTable = model.filteredItems
    var summary = s"Currently showing ${model.shownFolder().path}. "
    summary += s"${itemsInTable.size} files found, ${Size(itemsInTable.map(_.size.value).sum)} total size"
    if (!items.isEmpty) {
      summary += s". Selected ${items.size}, ${Size(items.map(_.size.value).sum)} total size."
    }
    browserInfo.text = summary
  }

  searchText.onChange { (_, _, text) =>
    if (text == "") {
      model.filteredItems.predicate = { x: ObservableBackupPart => true }
    } else {
      model.filteredItems.predicate = { x: ObservableBackupPart =>
        x.backupPart.name.toLowerCase().contains(text.toLowerCase)
      }
    }
  }

  browserTree.selectionModel().selectionMode = SelectionMode.Single
  var selectionModelListenerSuspended = false
  browserTree.selectionModel().selectedItem.onChange(
    (_, _, newTreeItem) => {
      if (!selectionModelListenerSuspended)
        model.shownFolder() = newTreeItem.getValue.backupPart
    }
  )

  model.shownFolder.onChange { (_, oldSelection, newSelection) =>
    val newItem = model.treeItems(newSelection)
    val oldItem = model.treeItems.get(oldSelection)

    selectionModelListenerSuspended = true

    def collapseOrExpand(item: TreeItem[BackupTreeNode], expand: Boolean) {
      var parent = item
      while (parent != null) {
        parent.expanded = expand
        parent = parent.parent.value
      }
    }
    oldItem.foreach(collapseOrExpand(_, false))
    collapseOrExpand(newItem, true)
    browserTree.selectionModel().select(newItem)
    selectionModelListenerSuspended = false
  }

  TableColumns.initTable(browserTable, model)

  restoreButton.disable.bind {
    Bindings.createBooleanBinding({ () =>
      selected.value match {
        case SelectedBoth(_, _) | NoneSelected => true
        case _ => false
      }
    }, selected)
  }

  restoreButton.text.bind {
    Bindings.createStringBinding({ () =>
      selected.value match {
        case SelectedFile(_) => "Restore file"
        case SelectedFolder(_) => "Restore folder"
        case SelectedFolders(x) => s"Restore ${x.size} folders"
        case SelectedFiles(x) => s"Restore ${x.size} files"
        case SelectedBoth(_, _) => "Select either files or folders"
        case NoneSelected => "Select files or folders to restore"
      }
    }, selected)
  }

  def preview(): Unit = {
    selected.value match {
      case SelectedFile(fd) =>
        restoreControllerI.preview(fd)
      case _ => println("No preview")
    }
  }

  val menu = new ContextMenu()
  val factories = mutable.Buffer(
    new PreviewActionFactory(),
    new RestoreToOriginalPlaceActionFactory(),
    new ShowFilesFromSubfoldersActionFactory()
  )

  def tableContextMenu(event: ContextMenuEvent): Unit = {
    val items = factories
      .map(fac => fac.createAction(selected(), BrowserController.this))
      .filter(_.isEnabled)
      .map(_.asMenuItem())

    if (!items.isEmpty) {
      menu.items.clear()
      menu.items ++= items.map(_.delegate)
      menu.show(browserTable, event.getScreenX(), event.getScreenY())
      event.consume()
    }
  }

  def hideMenu(): Unit = {
    menu.hide()
  }

  def startRestore(event: ActionEvent) {
    println("Handling restore")
  }

  def restoreToOriginal(items: Seq[BackupPart]): Unit = {
    // TODO
//    val configArgs = model.index.universe.config().asCommandLineArgs()
//    // TODO this is quite error prone and ugly
//    val conf = new RestoreConf(Seq("--restore-to-original-path") ++ configArgs)
//    conf.verify()
//    items.foreach { item =>
//      item match {
//        case fd : FileDescription =>
//          model.index.restoreFileDesc(fd)(conf)
//        case fd : FolderDescription =>
//          model.index.restoreFolderDesc(fd)(conf)
//      }
//    }
  }

  if (System.getProperty("user.name") == "Stivo") {
    showSubfolderFiles.selected = true
    browserSearch.text = ""
  }

}