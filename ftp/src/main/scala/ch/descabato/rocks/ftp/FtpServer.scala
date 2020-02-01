package ch.descabato.rocks.ftp

import java.io.File
import java.io.InputStream
import java.io.OutputStream
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.util

import ch.descabato.core.config.BackupFolderConfiguration
import ch.descabato.rocks.FileMetadataKeyWrapper
import ch.descabato.rocks.Revision
import ch.descabato.rocks.RocksEnv
import ch.descabato.rocks.protobuf.keys.BackedupFileType
import ch.descabato.rocks.protobuf.keys.FileMetadataValue
import ch.descabato.rocks.protobuf.keys.RevisionValue
import org.apache.ftpserver.FtpServerFactory
import org.apache.ftpserver.ftplet.FileSystemFactory
import org.apache.ftpserver.ftplet.FileSystemView
import org.apache.ftpserver.ftplet.FtpFile
import org.apache.ftpserver.ftplet.User
import org.apache.ftpserver.listener.ListenerFactory
import org.apache.ftpserver.usermanager.PropertiesUserManagerFactory
import org.apache.ftpserver.usermanager.impl.BaseUser

import scala.collection.immutable.TreeMap
import scala.jdk.CollectionConverters._

object FtpServer {

  def main(args: Array[String]): Unit = {
    val rootFolder = "l:/backup"

    val config = BackupFolderConfiguration(new File(rootFolder))
    serve(config, 8021)
  }

  def serve(config: BackupFolderConfiguration, port: Int): Unit = {
    val env = RocksEnv(config, readOnly = true)
    val reader = new BackupReader(env)
    val factory = new ListenerFactory()
    factory.setPort(port)
    val map = Map("default" -> factory.createListener())
    val serverFactory = new FtpServerFactory()
    val um = new PropertiesUserManagerFactory().createUserManager()
    val user = new BaseUser()
    user.setEnabled(true)
    user.setName("test")
    user.setPassword("test")
    user.setHomeDirectory("l:/")
    um.save(user)
    serverFactory.setUserManager(um)
    serverFactory.setListeners(map.asJava)
    serverFactory.createServer().start()
    serverFactory.setFileSystem(new MyFileSystemFactory(reader))
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

  def formatFilename(fileMetadataKeyWrapper: FileMetadataKeyWrapper): String = {
    val time = LocalDateTime.ofInstant(Instant.ofEpochMilli(fileMetadataKeyWrapper.fileMetadataKey.changed), ZoneId.systemDefault())
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

class FileNode(val name: String, val fileMetadataKeyWrapper: FileMetadataKeyWrapper, val metadata: FileMetadataValue) extends TreeNode {
  var linkCount = 1

  override def toString: String = s"File: ${fileMetadataKeyWrapper.fileMetadataKey.path} with length ${metadata.length}"
}

class FileFolder(val name: String, var backupInfo: Option[(FileMetadataKeyWrapper, FileMetadataValue)] = None) extends FolderNode {
  private var map: TreeMap[String, TreeNode] = TreeMap.empty

  def addSubfolder(fileFolder: FileFolder): Unit = {
    map += fileFolder.name -> fileFolder
  }

  def find(path: Seq[String]): Option[TreeNode] = {
    if (path.isEmpty) {
      Some(this)
    } else {
      children.find(_.name == path.head) match {
        case Some(x: FileFolder) =>
          x.find(path.tail)
        case Some(x: FileNode) if path.length == 1 =>
          Some(x)
        case _ =>
          None
      }
    }
  }

  def add(restPath: Seq[String], key: FileMetadataKeyWrapper, metadata: FileMetadataValue): Unit = {
    if (restPath.length == 1) {
      metadata.filetype match {
        case BackedupFileType.FOLDER if map.contains(restPath.last) =>
          map(restPath.last).asInstanceOf[FileFolder].backupInfo = Some((key, metadata))
        case BackedupFileType.FOLDER =>
          map += restPath.last -> new FileFolder(restPath.head, Some(key, metadata))
        case BackedupFileType.FILE if map.contains(restPath.last) =>
          map(restPath.last).asInstanceOf[FileNode].linkCount += 1
        case BackedupFileType.FILE =>
          map += restPath.last -> new FileNode(restPath.head, key, metadata)
      }
    } else {
      val folderName = restPath.head
      if (!map.contains(folderName)) {
        val ff = new FileFolder(folderName)
        map += ff.name -> ff
      }
      map(folderName).asInstanceOf[FileFolder].add(restPath.tail, key, metadata)
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
    val files: Seq[(FileMetadataKeyWrapper, FileMetadataValue)] = filesInRevision
      .flatMap(x => rocks.readFileMetadata(FileMetadataKeyWrapper(x))
        .map(y => (FileMetadataKeyWrapper(x), y)))
    files.foreach {
      case (x, y) =>
        val key = x.fileMetadataKey
        val pathAsSeq = PathUtils.splitPath(key.path)
        revisionParent.add(pathAsSeq, x, y)
        val newFilename = if (x.fileMetadataKey.filetype == BackedupFileType.FILE) {
          val filename = PathUtils.nameFromPath(key.path)
          val filenameSplit = filename.split('.')
          val timestamp = PathUtils.formatFilename(x).format(key.changed)
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
        allFiles.add(newFilename, x, y)
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

class MyFileSystemFactory(reader: BackupReader) extends FileSystemFactory {
  override def createFileSystemView(user: User): FileSystemView = {
    new MyFileSystemView(reader, user)
  }
}

class MyFileSystemView(reader: BackupReader, user: User) extends FileSystemView {

  val homeDirectory: MyFtpCommon = {
    MyFtpCommon.transformTree(reader.rootDirectory, reader)
  }

  var workingDirectory: MyFtpCommon = homeDirectory

  override def getHomeDirectory: FtpFile = homeDirectory

  override def getWorkingDirectory(): MyFtpCommon = workingDirectory

  override def changeWorkingDirectory(dir: String): Boolean = {
    if (dir == "..") {
      workingDirectory = homeDirectory.resolve(PathUtils.splitPath(workingDirectory.path).init)
    } else {
      val resolveFrom =
        if (dir.startsWith("/")) {
          // absolute change relative to home directory
          homeDirectory
        } else {
          // relative change to current working directory
          workingDirectory
        }
      workingDirectory = resolveFrom.resolve(PathUtils.splitPath(dir))
    }
    true
  }

  override def getFile(file: String): FtpFile = getWorkingDirectory().resolve(PathUtils.splitPath(file))

  override def isRandomAccessible: Boolean = false

  override def dispose(): Unit = {

  }
}

abstract class MyFtpCommon extends FtpFile {
  var lastModified = System.currentTimeMillis()

  def resolve(path: Seq[String]): MyFtpCommon

  def path: String

  override def getAbsolutePath: String = path

  override def getName: String = PathUtils.nameFromPath(path)

  override def isHidden: Boolean = false

  override def doesExist(): Boolean = true

  override def isReadable: Boolean = true

  override def isWritable: Boolean = false

  override def isRemovable: Boolean = false

  override def getOwnerName: String = "test"

  override def getGroupName: String = "test"

  override def getLinkCount: Int = 0

  override def getLastModified: Long = lastModified

  override def setLastModified(time: Long): Boolean = {
    lastModified = time
    true
  }

  override def delete(): Boolean = false

  override def move(destination: FtpFile): Boolean = false

}

class RocksFtpFile(val path: String, key: FileMetadataKeyWrapper, metadataValue: FileMetadataValue, reader: BackupReader) extends MyFtpCommon {

  var linkCount = 1

  override def resolve(path: Seq[String]): MyFtpCommon = this

  override def isDirectory: Boolean = false

  override def isFile: Boolean = true

  override def getSize: Long = {
    metadataValue.length
  }

  override def getLastModified: Long = {
    if (path.startsWith("/" + PathUtils.allFilesPath)) {
      val hour = 1000L * 60 * 60
      12 * hour + (linkCount - 1) * 24 * hour
    } else {
      key.fileMetadataKey.changed
    }
  }

  override def getLinkCount: Int = linkCount

  override def getGroupName: String = s"links_$linkCount"

  override def getPhysicalFile: AnyRef = this

  override def mkdir(): Boolean = ???

  override def listFiles(): util.List[_ <: FtpFile] = ???

  override def createOutputStream(offset: Long): OutputStream = ???

  override def createInputStream(offset: Long): InputStream = {
    val out = reader.createInputStream(metadataValue)
    out.skip(offset)
    out
  }
}


class RocksFtpFolder(fileFolder: FileFolder, val path: String, backupInfo: => Option[(FileMetadataKeyWrapper, FileMetadataValue)]) extends MyFtpCommon {

  var map: TreeMap[String, MyFtpCommon] = TreeMap.empty

  override def isDirectory: Boolean = true

  override def isFile: Boolean = false

  override def getSize: Long = 0

  override def getPhysicalFile: AnyRef = this

  override def mkdir(): Boolean = false

  override def move(destination: FtpFile): Boolean = false

  override def listFiles(): util.List[_ <: FtpFile] = map.values.toList.asJava

  override def getLastModified: Long = {
    backupInfo.map(_._1.fileMetadataKey.changed).getOrElse(super.getLastModified)
  }

  override def createOutputStream(offset: Long): OutputStream = ???

  override def createInputStream(offset: Long): InputStream = ???

  override def resolve(pathArg: Seq[String]): MyFtpCommon = {
    (this.path, pathArg) match {
      case (_, Seq() | Seq(".")) =>
        this
      case ("/", Seq("" | ".")) =>
        this
      case ("/", "" +: head +: tail) =>
        map(head).resolve(tail)
      case (_, x +: tail) =>
        map(x).resolve(tail)
    }
  }
}

object MyFtpCommon {
  def cleanName(name: String): String = name.replaceAllLiterally(":", "_")

  def transformTree(rootDirectory: FileFolder, reader: BackupReader, parentPath: Seq[String] = Seq.empty): MyFtpCommon = {
    val root = new RocksFtpFolder(rootDirectory, PathUtils.normalizePath(parentPath), rootDirectory.backupInfo)
    rootDirectory.children.foreach {
      case file: FileNode =>
        val cleaned = file.name
        val newFile = new RocksFtpFile(PathUtils.normalizePath(parentPath :+ cleaned), file.fileMetadataKeyWrapper, file.metadata, reader)
        newFile.linkCount = file.linkCount
        root.map += cleaned -> newFile
      case folder: FileFolder =>
        val cleaned = cleanName(folder.name)
        root.map += cleaned -> transformTree(folder, reader, parentPath :+ cleaned)
    }
    root
  }

}