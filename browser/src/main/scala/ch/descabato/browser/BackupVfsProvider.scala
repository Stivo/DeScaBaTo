package ch.descabato.browser

import org.apache.commons.vfs2.Capability
import org.apache.commons.vfs2.FileSystemOptions
import org.apache.commons.vfs2.FileName
import java.util.Collection
import java.util.ArrayList
import org.apache.commons.vfs2.provider.FileProvider
import org.apache.commons.vfs2.provider.AbstractFileSystem
import org.apache.commons.vfs2.provider.AbstractFileName
import org.apache.commons.vfs2.FileObject
import org.apache.commons.vfs2.provider.AbstractLayeredFileProvider
import org.apache.commons.vfs2.provider.AbstractFileObject
import org.apache.commons.vfs2.{ FileType => VfsFileType }
import org.apache.commons.vfs2.provider.LayeredFileName
import java.util.Date
import ch.descabato.BackupFolderConfiguration
import ch.descabato.BackupPart
import ch.descabato.BlockStrategy
import ch.descabato.FileAttributes
import ch.descabato.FileDescription
import ch.descabato.FolderDescription
import ch.descabato.RestoreHandler
import org.apache.commons.vfs2.Capability.COMPRESS
import org.apache.commons.vfs2.Capability.GET_LAST_MODIFIED
import org.apache.commons.vfs2.Capability.GET_TYPE
import org.apache.commons.vfs2.Capability.LAST_MODIFIED
import org.apache.commons.vfs2.Capability.LIST_CHILDREN
import org.apache.commons.vfs2.Capability.READ_CONTENT
import org.apache.commons.vfs2.Capability.VIRTUAL
import org.apache.commons.vfs2.{FileType => VfsFileType}
import ch.descabato.Utils

object BackupVfsProvider {

  import Capability._
  val capabilities = List(
    GET_TYPE,
    LIST_CHILDREN,
    READ_CONTENT,
    LAST_MODIFIED,
    GET_LAST_MODIFIED,
    READ_CONTENT,
    COMPRESS,
    VIRTUAL);

  var indexes = Map[String, VfsIndex]()
}

class BackupVfsProvider extends AbstractLayeredFileProvider with FileProvider {

  def doCreateFileSystem(scheme: String, file: FileObject, fileSystemOptions: FileSystemOptions) = {
    val rootName =
      new LayeredFileName(scheme, file.getName(), FileName.ROOT_PATH, VfsFileType.FOLDER);
    new BackupFileSystem(rootName, file, fileSystemOptions);
  }

  def getCapabilities: Collection[Capability] = {
    val list = new ArrayList[Capability]()
    BackupVfsProvider.capabilities.foreach { e =>
      list.add(e)
    }
    list
  }

}

class BackupFileSystem(name: AbstractFileName, fo: FileObject, options: FileSystemOptions) extends AbstractFileSystem(
  name, fo, options) with Serializable with Utils {

  protected def addCapabilities(caps: Collection[Capability]) {
    BackupVfsProvider.capabilities.foreach {
      c => caps.add(c)
    }
  }

  protected def createFile(name: AbstractFileName): FileObject = {
    val path = Utils.normalizePath(name.toString().drop("backup:file://".length()).takeWhile(_ != '!'))
    val index = BackupVfsProvider.indexes(path)
    new BackupFileObject(name, index, this);
  }
}

case class FakeBackupFolder(val path: String) extends BackupPart {
  def isFolder = true
  val attrs: FileAttributes = null
  val size = 0L
}

class BackupFileObject(name: AbstractFileName, index: VfsIndex, fs: AbstractFileSystem) extends AbstractFileObject(name, fs) with FileObject {

  // On windows, the url starts with !/c:/, and the paths in the backup start with c:
  // On linux, only the ! needs to be taken out
  val dropInUrl = if (Utils.isWindows) 2 else 1
  
  val nameInBackup = name.toString.dropWhile(_ != '!').drop(dropInUrl)

  def pathParts(x: Any): Seq[String] = x match {
    case x: String => x.split("[\\/]")
    case Some(x: BackupPart) => x.path.split("[\\/]")
    case x => println("Did not match " + x); Nil
  }

  protected override def doGetLastModifiedTime() = {
    if (backupPart.attrs != null) {
      backupPart.attrs.get("lastModifiedTime").toString.toLong
    } else {
      new Date().getTime()
    }
  }

  lazy val childrenOfPath = {
    val out = if (nameInBackup == "") {
      index.files.map(_.path.take(3)).toSet.toSeq.sorted
    } else {
      val candidates = index.files.filter(x => Utils.normalizePath(x.path).startsWith(nameInBackup))
      candidates.map(_.path.drop(nameInBackup.length()).dropWhile(_=='/').takeWhile(x => x != '\\' && x != '/')).toSet.toSeq.sorted
    }
    out.filter(_ != "")
  }

  lazy val backupPart: BackupPart = {
    index.files.find(x => Utils.normalizePath(x.path) == nameInBackup) match {
      case Some(x) => x
      case None => {
        new FakeBackupFolder(nameInBackup)
      }
    }
  }

  /**
   * Determines if this file can be written to.
   *
   * @return <code>true</code> if this file is writeable, <code>false</code> if not.
   * @throws FileSystemException if an error occurs.
   */
  override def isWriteable() = {
    false;
  }

  /**
   * Returns the file's type.
   */
  def doGetType() = {
    backupPart match {
      case x: FolderDescription => VfsFileType.FOLDER
      case x: FileDescription => VfsFileType.FILE
      case x: FakeBackupFolder => VfsFileType.FOLDER
      case _ => VfsFileType.IMAGINARY
    }
  }

  /**
   * Lists the children of the file.
   */
  @Override
  def doListChildren() = {
    childrenOfPath.toArray
  }

  protected def doGetContentSize() = backupPart.size

  protected def doGetInputStream() = index.getInputStream(backupPart.asInstanceOf[FileDescription])

}

class VfsIndex(config: BackupFolderConfiguration)
  extends RestoreHandler(config) {
  self: BlockStrategy =>

  val files = loadOldIndex()

  def registerIndex() {
    l.info("Loading information")
    val path = config.folder.getCanonicalPath()
    l.info("Path is loaded as " + path)
    BackupVfsProvider.indexes += Utils.normalizePath(path) -> this
  }

  var setup = false
  
  override def getInputStream(fd: FileDescription) = {
    if (!setup) {
      importOldHashLists
      setup = true
    }
    super.getInputStream(fd)
  }
  
}