package ch.descabato.ui

import java.io.InputStream
import java.util.Date

import ch.descabato.core.Universe
import ch.descabato.core.actors.MetadataStorageActor.BackupDescription
import ch.descabato.core.commands.DoReadAbstract
import ch.descabato.core_old._

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class Index(universe: Universe)
  extends DoReadAbstract(universe, false) {

  def versions = universe.context.fileManagerNew.backup.getDates()

  private var _backup = new LoadedBackup(Await.result(universe.metadataStorageActor.retrieveBackup(None), 1.minute))

  def backup = _backup

  def loadBackup(date: Date): Unit = {
    universe.metadataStorageActor.retrieveBackup(Some(date)).foreach( d =>
      _backup = new LoadedBackup(d)
    )
  }

  def getInputStream(fd: FileDescription): InputStream = {
    val stored = _backup.metadataFor(fd)
    getInputStream(stored)
  }

}

class LoadedBackup(val backup: BackupDescription) {
  //  val parts = backup.allParts
  val map = (backup.files.map(_.fd) ++ backup.folders).map(x => (x.path, x)).toMap
  val fileMetadatas = backup.files.map(x => (x.path, x)).toMap

  def metadataFor(fd: FileDescription) = {
    fileMetadatas(fd.path)
  }

  val tree: BackupTreeNode = {
    val root = BackupTreeNode(FakeBackupFolder("/"))
    backup.folders.foreach {
      root.insert
    }
    backup.files.foreach { f =>
      root.insert(f.fd)
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