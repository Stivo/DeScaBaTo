package ch.descabato.rocks.fuse

import java.io.InputStream
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import ch.descabato.rocks.FileMetadataKeyWrapper
import ch.descabato.rocks.Revision
import ch.descabato.rocks.RocksEnv
import ch.descabato.rocks.protobuf.keys.BackedupFileType
import ch.descabato.rocks.protobuf.keys.FileMetadataKey
import ch.descabato.rocks.protobuf.keys.FileMetadataValue
import ch.descabato.rocks.protobuf.keys.RevisionValue

import scala.collection.immutable.TreeMap

class BackupReader(val rocksEnv: RocksEnv) {

  import rocksEnv._

  lazy val revisions: Seq[RevisionPair] = {
    rocks.getAllRevisions()
      .map { case (x, y) =>
        RevisionPair(x, y)
      }
  }

  def createInputStream(metadataValue: FileMetadataValue): InputStream = {
    reader.createInputStream(metadataValue)
  }

  def listNodesForRevision(value: RevisionValue, revisionParent: FileFolder, allFiles: FileFolder): Unit = {
    val filesInRevision = value.files
    filesInRevision.foreach {
      key =>
        val pathAsSeq = PathUtils.splitPath(key.path)
        revisionParent.add(pathAsSeq, key)
        val newFilename = if (key.filetype == BackedupFileType.FILE) {
          val filename = PathUtils.nameFromPath(key.path)
          val filenameSplit = filename.split('.')
          val timestamp = PathUtils.formatFilename(key).format(key.changed)
          if (filenameSplit.length <= 1 || (filenameSplit.length == 2 && filenameSplit.head == "")) {
            val newFilename = filename + "_" + timestamp
            pathAsSeq.init :+ newFilename
          } else {
            val extension = "." + filenameSplit.last
            val newFilename = filename.dropRight(extension.length) + "_" + timestamp + extension
            pathAsSeq.init :+ newFilename
          }
        } else {
          pathAsSeq
        }
        allFiles.add(newFilename, key)
    }
  }


  val rootDirectory: FileFolder = {
    val directory = new FileFolder("/")
    val allFiles = new FileFolder(PathUtils.allFilesPath)
    revisions.foreach { revision =>
      val revisionParent = new FileFolder(revision.asName())
      listNodesForRevision(revision.revisionValue, revisionParent, allFiles)
      directory.addSubfolder(revisionParent)
    }
    directory.addSubfolder(allFiles)
    directory
  }

}

object PathUtils {
  def splitPath(path: String): Seq[String] = {
    val out = path.split("[\\\\/]")
    out
  }

  def normalizePath(path: String): String = normalizePath(splitPath(path))

  def normalizePath(path: Seq[String]): String = path.mkString("/", "/", "")

  def nameFromPath(path: String): String = splitPath(path).last

  private val dateFormatFolder = DateTimeFormatter.ofPattern("yyyy-MM-dd HH-mm")

  private val dateFormatFile = DateTimeFormatter.ofPattern("yyyy-MM-dd HH-mm-ss")

  def formatDate(revisionPair: RevisionPair): String = {
    val time = LocalDateTime.ofInstant(Instant.ofEpochMilli(revisionPair.revisionValue.created), ZoneId.systemDefault())
    dateFormatFolder.format(time)
  }

  def formatFilename(fileMetadataKeyWrapper: FileMetadataKey): String = {
    val time = LocalDateTime.ofInstant(Instant.ofEpochMilli(fileMetadataKeyWrapper.changed), ZoneId.systemDefault())
    dateFormatFile.format(time)
  }

  val allFilesPath = "All Files"
}

trait TreeNode {
  def name: String
}

trait FolderNode extends TreeNode {
  def children: Iterable[TreeNode]
}

class FileNode(val name: String, val fileMetadataKey: FileMetadataKey) extends TreeNode {
  var linkCount = 1

  def asWrapper: FileMetadataKeyWrapper = FileMetadataKeyWrapper(fileMetadataKey)

  override def toString: String = s"File: ${fileMetadataKey.path}"
}

class FileFolder(val name: String, var backupInfo: Option[FileMetadataKey] = None) extends FolderNode {
  private var map: TreeMap[String, TreeNode] = TreeMap.empty

  def addSubfolder(fileFolder: FileFolder): Unit = {
    map += fileFolder.name -> fileFolder
  }

  def find(path: Seq[String]): Option[TreeNode] = {
    if (path.isEmpty) {
      Some(this)
    } else {
      map.get(path.head) match {
        case Some(x: FileFolder) =>
          x.find(path.tail)
        case Some(x: FileNode) if path.length == 1 =>
          Some(x)
        case _ =>
          None
      }
    }
  }

  def add(restPath: Seq[String], key: FileMetadataKey): Unit = {
    if (restPath.length == 1) {
      key.filetype match {
        case BackedupFileType.FOLDER if map.contains(restPath.last) =>
          map(restPath.last).asInstanceOf[FileFolder].backupInfo = Some(key)
        case BackedupFileType.FOLDER =>
          map += restPath.last -> new FileFolder(restPath.head, Some(key))
        case BackedupFileType.FILE if map.contains(restPath.last) =>
          map(restPath.last).asInstanceOf[FileNode].linkCount += 1
        case BackedupFileType.FILE =>
          map += restPath.last -> new FileNode(restPath.head, key)
      }
    } else {
      val folderName = restPath.head
      if (!map.contains(folderName)) {
        val ff = new FileFolder(folderName)
        map += ff.name -> ff
      }
      map(folderName).asInstanceOf[FileFolder].add(restPath.tail, key)
    }
  }

  override def children: Iterable[TreeNode] = {
    map.values
  }

  override def toString: String = {
    s"${name} with ${children.size} children"
  }
}


case class RevisionPair(val revision: Revision, val revisionValue: RevisionValue) {

  def asName(): String = {
    val timestamp = PathUtils.formatDate(this)

    s"Files on $timestamp - Revision ${revision.number}"
  }

}