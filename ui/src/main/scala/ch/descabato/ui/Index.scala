package ch.descabato.ui

import java.io.{InputStream, SequenceInputStream}
import java.util.Date

import ch.descabato.core._
import ch.descabato.utils.CompressedStream

class Index(universe: Universe)
  extends RestoreHandler(universe) {

  def versions = universe.fileManager().getBackupDates()

  private var _backup = new LoadedBackup(universe.backupPartHandler().loadBackup())

  def backup = _backup

  def loadBackup(date: Date): Unit = {
    _backup = new LoadedBackup(universe.backupPartHandler().loadBackup(Some(date)))
  }

  def getInputStream(fd: FileDescription): InputStream = {
    val e = new java.util.Enumeration[InputStream]() {
      val it = getHashlistForFile(fd).iterator
      override def hasMoreElements: Boolean = it.hasNext

      override def nextElement(): InputStream = {
        val b = universe.blockHandler().readBlock(it.next())
        CompressedStream.decompress(b)
      }
    }
    new SequenceInputStream(e)
  }

}

class LoadedBackup(val backup: BackupDescription) {

  val parts = backup.allParts
  val map = backup.asMap

  val tree: BackupTreeNode = {
    val root = new BackupTreeNode(new FakeBackupFolder("/"))
    parts.foreach { part =>
      root.insert(part)
    }
    root
  }

}

case class BackupTreeNode(var backupPart: BackupPart, var children: Map[String, BackupTreeNode] = Map.empty) {
  def insert(part: BackupPart): Unit = {
    insert(part, part.pathParts)
  }
  def insert(part: BackupPart, restPath: Seq[String]): Unit = {
    if (restPath.length == 1) {
      val child = new BackupTreeNode(part)
      children.get(restPath.head) match {
        case None => children += restPath.head -> child
        case Some(node) => node.backupPart = part
      }
    } else {
      if (!(children contains restPath.head)) {

        var path = restPath.head
        val pathParts = part.pathParts.dropRight(restPath.length)
        if (pathParts.isEmpty) {
          path = restPath.head
        } else {
          path = pathParts.mkString("/") + "/" + restPath.head
        }
        val newNode = new BackupTreeNode(new FakeBackupFolder(path))
        children += restPath.head -> newNode
      }
      children(restPath.head).insert(part, restPath.drop(1))
    }
  }

  def lookup(restPath: Seq[String]): BackupTreeNode = {
    if (restPath.isEmpty) {
      this
    } else {
      children(restPath.head).lookup(restPath.drop(1))
    }
  }

  def tryLookup(restPath: Seq[String]): BackupTreeNode = {
    if (restPath.isEmpty || !children.contains(restPath.head)) {
      this
    } else {
      children(restPath.head).tryLookup(restPath.drop(1))
    }
  }

  override def toString: String = this.backupPart.name
}

case class FakeBackupFolder(val path: String) extends BackupPart {
  def isFolder = true
  val attrs: FileAttributes = null
  val size = 0L
  override def name = if (path == "/") "/" else super.name
}