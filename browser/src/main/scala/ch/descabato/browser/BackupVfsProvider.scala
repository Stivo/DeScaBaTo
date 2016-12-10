package ch.descabato.browser

import java.io.{SequenceInputStream, InputStream}
import java.util
import java.util.{ArrayList, Collection, Date}

import ch.descabato.core.{FileDescription, FolderDescription, _}
import ch.descabato.utils.{CompressedStream, Utils}
import org.apache.commons.vfs2.provider.{AbstractFileName, AbstractFileObject, AbstractFileSystem, AbstractLayeredFileProvider, FileProvider, LayeredFileName}
import org.apache.commons.vfs2.{Capability, FileName, FileObject, FileSystemOptions, FileType => VfsFileType}

object BackupVfsProvider {

  import org.apache.commons.vfs2.Capability._
  val capabilities = List(
    GET_TYPE,
    LIST_CHILDREN,
    READ_CONTENT,
    LAST_MODIFIED,
    GET_LAST_MODIFIED,
    READ_CONTENT,
    COMPRESS,
    VIRTUAL)

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
    val dropMore = if (Utils.isWindows) 1 else 0
    val path = Utils.normalizePath(name.toString().drop("backup:file://".length()).drop(dropMore).takeWhile(_ != '!'))
    val index = BackupVfsProvider.indexes(path)
    new BackupFileObject(name, index, this);
  }
}

case class FakeBackupFolder(val path: String) extends BackupPart {
  def isFolder = true
  val attrs: FileAttributes = null
  val size = 0L
}

class BackupFileObject(name: AbstractFileName, index: VfsIndex, fs: AbstractFileSystem)
	extends AbstractFileObject(name, fs) with FileObject with Utils {

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

  def isPathChar(x: Char) = x == '/' || x == '\\'

  def nextPathPart(x: String) = x.drop(nameInBackup.length()).dropWhile(isPathChar).takeWhile(!isPathChar(_))

  lazy val childrenOfPath = {
    val out = if (nameInBackup == "" && Utils.isWindows) {
      index.parts.map(_.path.take(3)).toSet.toSeq.sorted
    } else {
      val candidates = index.parts.filter(x => Utils.normalizePath(x.path).startsWith(nameInBackup))
      candidates.map(x => nextPathPart(x.path)).toSet.toSeq.sorted
    }
    out.filter(_ != "")
  }

  lazy val backupPart: BackupPart = {
    index.parts.find(x => Utils.normalizePath(x.path) == nameInBackup) match {
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

class VfsIndex(universe: Universe)
  extends RestoreHandler(universe) {

  val backup = universe.backupPartHandler().loadBackup()
  val parts = backup.allParts

  def registerIndex() {
    l.info("Loading information")
    val path = config.folder.getCanonicalPath()
    l.info("Path is loaded as " + path)
    BackupVfsProvider.indexes += Utils.normalizePath(path) -> this
  }

  def getInputStream(fd: FileDescription): InputStream = {
    val e = new util.Enumeration[InputStream]() {
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
