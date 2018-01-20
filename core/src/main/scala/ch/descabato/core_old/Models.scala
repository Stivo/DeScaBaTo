package ch.descabato.core_old

import java.io.{File, IOException}
import java.math.{BigDecimal => JBigDecimal}
import java.nio.file.attribute._
import java.nio.file.{Files, LinkOption, Path}
import java.security.{MessageDigest, Principal}
import java.util
import java.util.regex.Pattern

import akka.util.ByteString
import ch.descabato.CompressionMode
import ch.descabato.core.util.{FileReader, FileWriter, SimpleFileReader, SimpleFileWriter}
import ch.descabato.remote.RemoteOptions
import ch.descabato.utils.Implicits._
import ch.descabato.utils._
import com.fasterxml.jackson.annotation.JsonIgnore

import scala.collection.JavaConverters._

case class BackupFolderConfiguration(folder: File, prefix: String = "", @JsonIgnore var passphrase: Option[String] = None, newBackup: Boolean = false) {

  def this() = this(null)
  @JsonIgnore
  var configFileName: String = prefix + "backup.json"
  var version: String = ch.descabato.version.BuildInfo.version
  var serializerType = "json"
  @JsonIgnore
  def serialization(typ: String = serializerType): AbstractJacksonSerialization = typ match {
    case "smile" => new SmileSerialization
    case "json" => new JsonSerialization
  }
  var keyLength = 128
  var compressor = CompressionMode.smart
  def hashLength: Int = createMessageDigest().getDigestLength()
  var hashAlgorithm = "MD5"
  @JsonIgnore def createMessageDigest(): MessageDigest = MessageDigest.getInstance(hashAlgorithm)
  var blockSize: Size = Size("16Kb")
  var volumeSize: Size = Size("100Mb")
  var threads: Int = 1
//  val useDeltas = false
  var hasPassword: Boolean = passphrase.isDefined
//  var renameDetection = true
//  var redundancyEnabled = false
//  var metadataRedundancy: Int = 20
//  var volumeRedundancy: Int = 5
  var saveSymlinks: Boolean = true
  var createIndexes: Boolean = true
  var ignoreFile: Option[File] = None

  var remoteOptions: RemoteOptions = new RemoteOptions()

  var key: ByteString = _

  def newWriter(file: File): FileWriter = {
    if (key == null) {
      new SimpleFileWriter(file)
    } else {
      ???
      //new EncryptedFileWriter(file, key)
    }
  }

  def newReader(file: File): FileReader = {
    if (key == null) {
      new SimpleFileReader(file)
    } else {
      ???
      //new EncryptedFileReader(file, key)
    }
  }

  def relativePath(file: File): String = {
    folder.toPath.relativize(file.toPath).toString.replace('\\', '/')
  }

  def verify(): Unit = {
    remoteOptions.verify()
  }


}


class FileAttributes extends util.HashMap[String, Any] with Utils {

  def hasBeenModified(file: File): Boolean = {
    val fromMap = get("lastModifiedTime")
    val lastMod = {
      Files.getAttribute(file.toPath(), "lastModifiedTime", LinkOption.NOFOLLOW_LINKS) match {
        case x: FileTime => x.toMillis()
        case _ => l.warn("Did not find filetime for " + file); 0L
      }
    }

    val out = Option(fromMap) match {
      case Some(l: Long) => !(lastMod <= l && l <= lastMod)
      case Some(l: Int) => !(lastMod <= l && l <= lastMod)
      case Some(ft: String) =>
        val l = ft.toLong; !(lastMod <= l && l <= lastMod)
      case None => true
      case x => println("Was modified: " + lastMod + " vs " + x + " " + x.getClass + " "); true
    }
    out
  }

}

case class MetadataOptions(saveMetadata: Boolean = false)

object FileAttributes extends Utils {

  val posixGroup = "posix:group"
  val posixPermissions = "posix:permissions"
  val owner = "owner"
  val lastModified = "lastModifiedTime"
  val creationTime = "creationTime"

  private def readAttributes[T <: BasicFileAttributes](path: Path)(implicit m: Manifest[T]) =
    Files.readAttributes[T](path, m.runtimeClass.asInstanceOf[Class[T]], LinkOption.NOFOLLOW_LINKS)

  def apply(path: Path): FileAttributes = {
    val out = new FileAttributes()
    def add(attr: String, o: Any) = o match {
      case ft: FileTime => out.put(attr, ft.toMillis())
      case x: Boolean => out.put(attr, x)
      case x: String => out.put(attr, x)
      case p: Principal => out.put(attr, p.getName())
    }

    readBasicAttributes(path, add _)
    readPosixAttributes(add _, path)
    readDosAttributes(add _, path)

    out
  }

  private def readBasicAttributes(path: Path, add: (String, Any) => Any) = {
    val attrs = readAttributes[BasicFileAttributes](path)

    val keys = List(lastModified, creationTime)
    keys.foreach { k =>
      val m = attrs.getClass().getMethod(k)
      m.setAccessible(true)
      add(k, m.invoke(attrs))
    }
  }

  def readPosixAttributes(add: (String, Object) => Any, path: Path) = {
    try {
      val posix = readAttributes[PosixFileAttributes](path)
      if (posix != null) {
        add(owner, posix.owner())
        add(posixGroup, posix.group())
        add(posixPermissions, PosixFilePermissions.toString(posix.permissions()))
      }
    } catch {
      case e: UnsupportedOperationException => // ignore, not a posix system
    }
  }

  private def readDosAttributes(add: (String, Any) => Any, path: Path) = {
    try {
      val dos = readAttributes[DosFileAttributes](path)
      if (dos.isReadOnly) {
        add("dos:readonly", true)
      }
      if (dos.isHidden) {
        add("dos:hidden", true)
      }
      if (dos.isArchive) {
        add("dos:archive", true)
      }
      if (dos.isSystem) {
        add("dos:system", true)
      }
    } catch {
      case e: UnsupportedOperationException => // ignore, not a dos system
    }
  }

  def restore(attrs: FileAttributes, file: File) {
    val path = file.toPath()
    def lookupService = file.toPath().getFileSystem().getUserPrincipalLookupService()
    lazy val posix = Files.getFileAttributeView(path, classOf[PosixFileAttributeView])
    val dosOnes = "hidden,archive,readonly".split(",").map("dos:"+_).toSet
    for ((name, o) <- attrs.asScala) {
      try {
        val toSet: Option[Any] = (name, o) match {
          case (key, time) if key.endsWith("Time") => Some(FileTime.fromMillis(o.toString.toLong))
          case (s, group) if s == posixGroup =>
            val g = lookupService.lookupPrincipalByGroupName(group.toString)
            posix.setGroup(g)
            None
          case (s, group) if s == owner =>
            val g = lookupService.lookupPrincipalByName(group.toString)
            posix.setOwner(g)
            None
          case (s, perms) if s == posixPermissions =>
            val p = PosixFilePermissions.fromString(perms.toString)
            posix.setPermissions(p)
            None
          case (key, value) if key.startsWith("dos:") =>
            Some(value)
        }
        toSet.foreach { s =>
          Files.setAttribute(file.toPath(), name, s, LinkOption.NOFOLLOW_LINKS)
        }
      } catch {
        case e: IOException if Files.isSymbolicLink(path) => // Ignore, seems normal on linux
        case e: IOException => l.warn(s"Failed to restore attribute $name for file $file")
      }
    }
  }

}

trait UpdatePart {
  def size: Long
  def path: String
  @JsonIgnore def name: String = pathParts.last
  @JsonIgnore def pathParts: Array[String] = path.split("[\\\\/]")
}

case class FileDeleted(path: String) extends UpdatePart {
  def size = 0L
}

object FileDeleted {
  def apply(x: BackupPart): FileDeleted = x match {
    case FolderDescription(path, _) => new FileDeleted(path)
    case FileDescription(path, _, _, _) => new FileDeleted(path)
  }
}

case class FileDescription(path: String, size: Long, attrs: FileAttributes, hash: Hash = Hash.nul) extends BackupPart {

  def this(file: File) = {
    this(file.getAbsolutePath, file.length(), FileAttributes(file.toPath))
  }
  var hasHashList: Boolean = false

  @JsonIgnore def isFolder = false
  override def equals(x: Any): Boolean = x match {
    case FileDescription(p, s, attributes, h) if p == path && s == size && attributes == attrs => hash === h
    case _ => false
  }

  override def hashCode(): Int = {
    path.hashCode
  }
}

case class BlockId(file: FileDescription, part: Int)

class Block(val id: BlockId, val content: BytesWrapper) {
  val uncompressedLength: Int = content.length
  var hash: Hash = Hash.nul
  var mode: CompressionMode = _
  var compressed: BytesWrapper = _
}

object FolderDescription {
  def apply(dir: File): FolderDescription =  {
    if (!dir.isDirectory) throw new IllegalArgumentException(s"Must be a directory, $dir is a file")
    FolderDescription(dir.toString(), FileAttributes(dir.toPath))
  }
}

case class FolderDescription(path: String, attrs: FileAttributes) extends BackupPart {
  @JsonIgnore val size = 0L
  @JsonIgnore def isFolder = true
}

trait BackupPart extends UpdatePart {
  @JsonIgnore def isFolder: Boolean
  
  def attrs: FileAttributes
  def size: Long

  def applyAttrsTo(f: File) {
    FileAttributes.restore(attrs, f)
  }

}

case class SymbolicLink(path: String, linkTarget: String, attrs: FileAttributes) extends BackupPart {
  def size = 0L
  def isFolder = false
}

case class BackupDescription(files: Vector[FileDescription] = Vector.empty,
                             folders: Vector[FolderDescription] = Vector.empty,
                             symlinks: Vector[SymbolicLink] = Vector.empty, deleted: Vector[FileDeleted] = Vector.empty) {
  def merge(later: BackupDescription): BackupDescription = {
    val set = later.deleted.map(_.path).toSet ++ later.asMap.keySet
    def remove[T <: BackupPart](x: Vector[T]) = {
      x.filterNot(bp => set safeContains bp.path)
    }
    BackupDescription(remove(files) ++ later.files, remove(folders) ++ later.folders,
      remove(symlinks) ++ later.symlinks, later.deleted)
  }

  @JsonIgnore def allParts: Vector[BackupPart with Product with Serializable] = files ++ folders ++ symlinks

  @JsonIgnore lazy val asMap: Map[String, BackupPart] = {
    var map = Map[String, BackupPart]()
    allParts.foreach {x => map += ((x.path, x))}
    map
  }
  
  def + (x: UpdatePart): BackupDescription = x match {
    case x: FileDescription => this.copy(files = files :+ x)
    case x: FolderDescription => this.copy(folders = folders :+ x)
    case x: SymbolicLink => this.copy(symlinks = symlinks :+ x)
  }
  
  @JsonIgnore def size: Int = files.size + folders.size + symlinks.size
  
  @JsonIgnore def sizeWithDeleted: Int = size + deleted.size
  
  @JsonIgnore def isEmpty(): Boolean = sizeWithDeleted == 0
}

// Domain classes
case class Size(bytes: Long) {
  override def toString: String = Utils.readableFileSize(bytes)
}

object Size {
  val knownTypes: Set[Class[_]] = Set(classOf[Size])
  val patt: Pattern = Pattern.compile("([\\d.]+)[\\s]*([GMK]?B)", Pattern.CASE_INSENSITIVE)

  def apply(size: String): Size = {
    var out: Long = -1
    val matcher = patt.matcher(size)
    val map = List(("GB", 3), ("MB", 2), ("KB", 1), ("B", 0)).toMap
    if (matcher.find()) {
      val number = matcher.group(1)
      val pow = map.get(matcher.group(2).toUpperCase()).get
      var bytes = new BigDecimal(new JBigDecimal(number))
      bytes = bytes.*(BigDecimal.valueOf(1024).pow(pow))
      out = bytes.longValue()
    }
    new Size(out)
  }
}

case class MetaInfo(date: String, writingVersion: String)
