package ch.descabato.ui

import java.util.{Comparator, Date}

import ch.descabato.core.{BackupPart, FileDescription}

import scala.collection.mutable
import scalafx.beans.binding.Bindings
import scalafx.beans.property._
import scalafx.collections.ObservableBuffer
import scalafx.collections.transformation.{FilteredBuffer, SortedBuffer}
import scalafx.scene.control.TreeItem

object BackupViewModel {
  var index: Index = null
}

class BackupViewModel(val index: Index = BackupViewModel.index) {
  def indexUpdated() = {
    val backupPath = shownFolder()

    treeItems = Map.empty[BackupPart, TreeItem[BackupTreeNode]]
    root = createTreeItem(index.backup.tree)
    populateListForPath(index.backup.tree.backupPart, showSubfolders.value)
    shownFolder() = index.backup.tree.tryLookup(backupPath.pathParts).backupPart
  }

  val tableItems = new ObservableBuffer[ObservableBackupPart]()
  val filteredItems = new FilteredBuffer(tableItems)
  val sortedItems = new SortedBuffer(filteredItems) {
    comparator = Ordering.by(x => (x.backupPart.pathParts.init.mkString("/"), !x.backupPart.isFolder, x.backupPart.name))
  }

  val shownFolder = new ObjectProperty[BackupPart]()
  val showSubfolders = new BooleanProperty()

  val inputs = Bindings.createObjectBinding[(BackupPart, Boolean)]( { ()  =>
    (shownFolder.value, showSubfolders.value)
  }, shownFolder, showSubfolders)

  inputs.onChange {
    (_, _, newValues) => {
      populateListForPath(newValues._1, newValues._2)
    }
  }

  shownFolder() = index.backup.tree.backupPart

  def populateListForPath(backupPart: BackupPart, includeSubFolders: Boolean = false) = {
    tableItems.clear()
    val node = index.backup.tree.lookup(backupPart.pathParts)
    var buffer = mutable.Seq.empty[ObservableBackupPart]
    def addRecursive(node: BackupTreeNode): Unit = {
      node.children.map { case (_, childNode@BackupTreeNode(bp, _)) =>
        buffer :+= new ObservableBackupPart(bp)
        if (includeSubFolders) {
          addRecursive(childNode)
        }
      }
    }
    addRecursive(node)
    tableItems ++= buffer
  }

  def createTreeItem(node: BackupTreeNode): TreeItem[BackupTreeNode] = {
    new TreeItem[BackupTreeNode](node) {
      value = node
      treeItems += node.backupPart -> this
      children = node.children.filter(_._2.backupPart.isFolder).toSeq.sortBy(_._1).map(_._2).map {
        createTreeItem(_)
      }.toList

    }
  }

  var treeItems = Map.empty[BackupPart, TreeItem[BackupTreeNode]]

  var root = createTreeItem(index.backup.tree)
}

class ObservableBackupPart(val backupPart: BackupPart) {

  val name: StringProperty = StringProperty(backupPart.name)
  val extension: StringProperty = StringProperty({
    if (!backupPart.isFolder && backupPart.name.contains('.')) {
      val name = backupPart.name
      name.split('.').last
    } else ""
  })
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
