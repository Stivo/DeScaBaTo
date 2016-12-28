package ch.descabato.web

import java.io.{File, FileInputStream}

import ch.descabato.core.Size

import scalafx.Includes._
import scalafx.scene.control._
import scalafxml.core.macros.sfxml

@sfxml
class SearchController(
                         private val searchTable: TableView[ObservableBackupPart],
                         private val searchSearch: TextField,
                         private val searchInfo: Label
                       ) {
  val model = new BackupViewModel()
  println(model.root)
  TableColumns.initTable(searchTable, model)

  searchTable.items = model.sortedItems

  model.sortedItems.comparatorProperty().bind(searchTable.comparatorProperty())

  model.populateListWithAll()
  updateInfo()

  model.filteredItems.onChange {
    (_, changes) => {
      updateInfo()
    }
  }

  private def updateInfo() = {
    val items = model.sortedItems
    searchInfo.text = s"${items.size} files found, ${Size(items.map(_.size.value).sum)} total size"
  }

  searchSearch.textProperty.onChange { (_, _, text) =>
    println("Search updated "+text)
    if (text == "") {
      model.filteredItems.predicate = { _ => true}
    } else {
      val regex = ("(?i)"+text.replace("*", ".+")).r
      model.filteredItems.predicate = { x: ObservableBackupPart =>
        regex.findFirstIn(x.backupPart.path).isDefined
      }
    }
  }

}