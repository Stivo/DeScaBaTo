package ch.descabato.web


import ch.descabato.core.{BackupPart, FileDescription, Size}

import scalafx.event.ActionEvent
import scalafx.scene.control._
import scalafxml.core.macros.sfxml
import scalafx.Includes._
import scalafx.animation.PauseTransition
import scalafx.beans.binding.Bindings
import scalafx.beans.property.StringProperty
import scalafx.util.Duration

sealed trait RestoreOptions

case object RestoreNone extends RestoreOptions
case class RestoreFile(fileDescription: FileDescription) extends RestoreOptions
case class RestoreFiles(fileDescription: Seq[FileDescription]) extends RestoreOptions
case class RestoreFolder(folderDescription: BackupPart) extends RestoreOptions
case class RestoreFolders(folderDescription: Seq[BackupPart]) extends RestoreOptions
case object RestoreBoth extends RestoreOptions

@sfxml
class BrowserController(
                         private val browserTree: TreeView[BackupTreeNode],
                         private val browserTable: TableView[ObservableBackupPart],
                         private val browserSearch: TextField,
                         private val restoreButton: Button,
                         private val showSubfolderFiles: CheckBox,
                         private val browserInfo: Label
                       ) {
  val model = new BackupViewModel()
  browserTree.root = model.root
  browserTable.items = model.sortedItems

  model.showSubfolders.bind(showSubfolderFiles.selected)

  browserTable.onSort = { _ => model.sortedItems.comparatorProperty().bind(browserTable.comparatorProperty()) }

  val items = browserTable.selectionModel().getSelectedItems

  val searchText = new StringProperty("")

  val pause = new PauseTransition(Duration.apply(250))
  browserSearch.textProperty.onChange { (_, _, newValue) =>
    pause.onFinished = _ => searchText.value = newValue
    pause.playFromStart()
  }

  val restoreOption = Bindings.createObjectBinding[RestoreOptions]({ () =>
    if (items.isEmpty) {
      RestoreNone
    } else if (items.size() == 1) {
      if (items(0).backupPart.isFolder) {
        RestoreFolder(items(0).backupPart)
      } else {
        RestoreFile(items(0).backupPart.asInstanceOf[FileDescription])
      }
    } else {
      val (folders, files) = items.partition(_.backupPart.isFolder)
      if (folders.size == 0) {
        RestoreFiles(items.map(_.backupPart.asInstanceOf[FileDescription]))
      } else if (files.size == 0) {
        RestoreFolders(items.map(_.backupPart).toSeq)
      } else {
        RestoreBoth
      }
    }
  }, items)

  updateInfo()

  model.filteredItems.onChange {
    (_, changes) => {
      updateInfo()
    }
  }

  private def updateInfo() = {
    val items = model.filteredItems
    browserInfo.text = s"${items.size} files found, ${Size(items.map(_.size.value).sum)} total size"
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
    val oldItem = model.treeItems(oldSelection)

    selectionModelListenerSuspended = true

    def collapseOrExpand(item: TreeItem[BackupTreeNode], expand: Boolean) {
      var parent = item
      while (parent != null) {
        parent.expanded = expand
        parent = parent.parent.value
      }
    }
    collapseOrExpand(oldItem, false)
    collapseOrExpand(newItem, true)
    browserTree.selectionModel().select(newItem)
    selectionModelListenerSuspended = false
  }

  TableColumns.initTable(browserTable, model)

  restoreButton.disable.bind {
    Bindings.createBooleanBinding({ () =>
      restoreOption.value match {
        case RestoreBoth | RestoreNone => true
        case _ => false
      }
    }, restoreOption)
  }

  restoreButton.text.bind {
    Bindings.createStringBinding({ () =>
      restoreOption.value match {
        case RestoreFile(_) => "Restore file"
        case RestoreFolder(_) => "Restore folder"
        case RestoreFolders(x) => s"Restore ${x.size} folders"
        case RestoreFiles(x) => s"Restore ${x.size} files"
        case RestoreBoth => "Select either files or folders"
        case RestoreNone => "Select files or folders to restore"
      }
    }, restoreOption)
  }

  def startRestore(event: ActionEvent) {
    println("Handling restore")
  }
}