package ch.descabato.web

import java.io.{File, FileInputStream}

import scalafx.event.ActionEvent
import scalafx.scene.control._
import scalafx.scene.image.{Image, ImageView}
import scalafx.scene.layout.GridPane
import scalafxml.core.macros.sfxml
import scalafx.Includes._
import scalafx.beans.property.StringProperty
import scalafx.scene.input.InputMethodEvent

@sfxml
class BrowserController(
                         private val browserTree: TreeView[BackupTreeNode],
                         private val browserTable: TableView[ObservableBackupPart],
                         private val browserSearch: TextField
                       ) {
  val model = new BackupViewModel()
  browserTree.root = model.root
  browserTable.items = model.sortedItems
  model.sortedItems.comparatorProperty().bind(browserTable.comparatorProperty())

  browserSearch.textProperty.onChange { (_, _, text) =>
    println("Search updated "+text)
    if (text == "") {
      model.filteredItems.predicate = { _ => true}
    } else {
      model.filteredItems.predicate = { _.backupPart.name.contains(text)}
    }
//    case x => println(s"Got $x instead")
  }
  model.populateListForPath(model.index.tree.backupPart)


  browserTree.selectionModel().selectionMode = SelectionMode.Single
  browserTree.selectionModel().selectedItem.onChange(
    (_, _, newTreeItem) => {
      model.populateListForPath(newTreeItem.getValue.backupPart)
      newTreeItem.expanded = true
    }
  )

  TableColumns.initTable(browserTable, model)
}