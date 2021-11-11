package ch.descabato.rocks.fuse

import ch.descabato.rocks.ChunkKey

import java.io.InputStream
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import ch.descabato.rocks.FileMetadataKeyWrapper
import ch.descabato.rocks.Revision
import ch.descabato.rocks.BackupEnv
import ch.descabato.rocks.protobuf.keys.BackedupFileType
import ch.descabato.rocks.protobuf.keys.FileMetadataKey
import ch.descabato.rocks.protobuf.keys.FileMetadataValue
import ch.descabato.rocks.protobuf.keys.RevisionValue
import ch.descabato.rocks.protobuf.keys.ValueLogIndex
import ch.descabato.utils.Utils

import scala.collection.immutable.SortedMap
import scala.collection.immutable.TreeMap

class BackupReader(val rocksEnv: BackupEnv) extends Utils {

  import rocksEnv._

  lazy val revisions: Seq[RevisionPair] = {
    rocks.getAllRevisions()
      .map { case (x, y) =>
        RevisionPair(x, y)
      }.toSeq
  }

  lazy val chunksByVolume: Map[Int, Map[ChunkKey, ValueLogIndex]] = {
    var out = SortedMap.empty[Int, Map[ChunkKey, ValueLogIndex]]
    rocksEnv.rocks.getAllChunks().foreach { case (key, v) =>
      val number = rocksEnv.fileManager.volume.numberOfFile(rocksEnv.config.resolveRelativePath(v.filename))
      var map = out.getOrElse(number, Map.empty)
      map += (key -> v)
      out += number -> map
    }
    out
  }

  def fileMetadataValuesByVolume(revision: Revision, x: Int): Map[String, (Long, Seq[Int])] = {
    val chunks = chunksByVolume(x)
    val rev = revisions.filter(_.revision == revision).head
    val revisionFiles = rev.revisionValue.files
    var out = Map.empty[String, (Long, Seq[Int])]
    for (revFile <- revisionFiles) {
      val value = rocks.readFileMetadata(FileMetadataKeyWrapper(revFile)).get
      for ((hash, index) <- rocks.getHashes(value).zipWithIndex) {
        if (chunks.contains(hash)) {
          //          println(revFile.path+" "+index+" "+chunks(hash).lengthCompressed)
          var length = out.getOrElse(revFile.path, (0L, Seq.empty))
          length = (chunks(hash).lengthCompressed + length._1, length._2 :+ index)
          out += revFile.path -> length
        }
      }
    }
    out
  }

  def createInputStream(metadataValue: FileMetadataValue): InputStream = {
    reader.createInputStream(metadataValue)
  }

  def listNodesForRevision(revisionPair: RevisionPair, revisionParent: FileFolder, addTimestampToName: Boolean): Unit = {
    val filesInRevision = revisionPair.revisionValue.files
    l.info(s"""Listing files for "${PathUtils.formatDate(revisionPair)} - Revision ${revisionPair.revision.number}" to add to ${revisionParent.name}""")

    filesInRevision.foreach {
      key =>
        val pathAsSeq = PathUtils.splitPath(key.path)
        val newFilename = if (addTimestampToName && key.filetype == BackedupFileType.FILE) {
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
        revisionParent.add(newFilename, key)
    }
  }

  val rootDirectory: FileFolder = {
    val directory = new FileFolder("/", _ => {})
    val allFiles = new FileFolder(PathUtils.allFilesPath, { ff =>
      revisions.foreach { revision =>
        listNodesForRevision(revision, ff, true)
      }
    })
    revisions.foreach { revision =>
      val revisionParent = new FileFolder(revision.asName(), ff => listNodesForRevision(revision, ff, false))
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

class FileFolder(val name: String, initFunction: FileFolder => Unit, var backupInfo: Option[FileMetadataKey] = None) extends FolderNode with Utils {
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
          map += restPath.last -> new FileFolder(restPath.head, _ => {}, Some(key))
        case BackedupFileType.FILE if map.contains(restPath.last) =>
          map(restPath.last).asInstanceOf[FileNode].linkCount += 1
        case BackedupFileType.FILE =>
          map += restPath.last -> new FileNode(restPath.head, key)
      }
    } else {
      val folderName = restPath.head
      if (!map.contains(folderName)) {
        val ff = new FileFolder(folderName, _ => {})
        map += ff.name -> ff
      }
      map(folderName).asInstanceOf[FileFolder].add(restPath.tail, key)
    }
  }

  private var isInitialized = false

  override def children: Iterable[TreeNode] = {

    this.synchronized {
      if (!isInitialized) {
        initFunction(this)
        isInitialized = true
      }
    }

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