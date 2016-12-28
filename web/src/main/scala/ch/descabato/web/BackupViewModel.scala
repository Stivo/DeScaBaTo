package ch.descabato.web

import java.util.Date
import javafx.scene.{control => jfxsc}

import ch.descabato.core.{BackupPart, FileDescription}

import scalafx.beans.property._
import scalafx.collections.ObservableBuffer
import scalafx.collections.transformation.{FilteredBuffer, SortedBuffer}
import scalafx.scene.control.TreeItem

object BackupViewModel {
  var index: Index = null
}

/**
 * @author Jarek Sacha
 */
class BackupViewModel(val index: Index = BackupViewModel.index) {
  val tableItems = new ObservableBuffer[ObservableBackupPart]()
  val filteredItems = new FilteredBuffer(tableItems)
  val sortedItems = new SortedBuffer(filteredItems)

  def populateListWithAll() = {
    index.parts.foreach {
      case f@FileDescription(_, _, _, _) => tableItems.add(new ObservableBackupPart(f))
      case _ => // ignore
    }
  }

  def populateListForPath(backupPart: BackupPart) = {
    tableItems.clear()
    val node = index.tree.lookup(backupPart.pathParts)
    println(node)
    node.children.map { case (_, BackupTreeNode(bp, _)) =>
      tableItems.add(new ObservableBackupPart(bp))
    }
  }

  def createTreeItem(node: BackupTreeNode): TreeItem[BackupTreeNode] = {
    new TreeItem[BackupTreeNode](node) {
      value = node

      children = node.children.filter(_._2.backupPart.isFolder).toSeq.sortBy(_._1).map(_._2).map {
        createTreeItem(_)
      }.toList

    }
  }

  lazy val root = createTreeItem(index.tree)
}

class ObservableBackupPart(val backupPart: BackupPart) {

  val name: StringProperty = StringProperty(backupPart.name)
  val path: StringProperty = StringProperty(backupPart.pathParts.init.mkString("/") )
  val size: ObjectProperty[Long] = new ObjectProperty[Long](backupPart, "size", backupPart.size)
  val `type`: StringProperty = StringProperty(if (backupPart.isFolder) "dir" else "file")

  val date = new ObjectProperty[Date](
    backupPart, "attrs",
    Option(backupPart.attrs)
      .flatMap(attrs => Option(attrs.get("lastModifiedTime")))
      .flatMap{ case l: Long => Some(l); case _ => None}
      .map(l => new Date(l))
      .orNull)
}
