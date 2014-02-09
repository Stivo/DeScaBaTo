package ch.descabato.core

import java.io.File
import java.util.HashMap
import scala.collection.JavaConverters._
import java.nio.file.Files
import java.nio.file.attribute.FileTime
import java.util.Arrays
import java.nio.file.attribute.BasicFileAttributes
import com.fasterxml.jackson.annotation.JsonIgnore
import java.util.regex.Pattern
import java.math.{ BigDecimal => JBigDecimal }
import java.nio.file.LinkOption
import java.nio.file.Path
import java.nio.file.attribute.PosixFileAttributes
import java.nio.file.attribute.PosixFilePermissions
import java.security.Principal
import java.nio.file.attribute.PosixFileAttributeView
import java.io.IOException
import ch.descabato.utils.SmileSerialization
import java.security.MessageDigest
import ch.descabato.utils.JsonSerialization
import ch.descabato.CompressionMode
import ch.descabato.utils.Utils
import scala.collection.mutable.Buffer
import scala.collection.mutable

case class BackupFolderConfiguration(folder: File, prefix: String = "", @JsonIgnore var passphrase: Option[String] = None, newBackup: Boolean = false) {
  def this() = this(null)
  @JsonIgnore
  var configFileName = prefix + "backup.json"
  var version = ch.descabato.version.BuildInfo.version
  var serializerType = "smile"
  @JsonIgnore
  def serialization(typ: String = serializerType) = typ match {
    case "smile" => new SmileSerialization
    case "json" => new JsonSerialization
  }
  var keyLength = 128
  var compressor = CompressionMode.gzip
  def hashLength = getMessageDigest().getDigestLength()
  var hashAlgorithm = "MD5"
  @JsonIgnore def getMessageDigest() = MessageDigest.getInstance(hashAlgorithm)
  var blockSize: Size = Size("16Kb")
  var volumeSize: Size = Size("100Mb")
  var threads: Int = 1
  var checkPointEvery: Size = volumeSize
  val useDeltas = false
  var hasPassword = passphrase.isDefined
  var renameDetection = true
  var redundancyEnabled = false
  var metadataRedundancy: Int = 20
  var volumeRedundancy: Int = 5
  var saveSymlinks: Boolean = true
  @JsonIgnore def raes = if (hasPassword) ".raes" else ""
}


class FileAttributes extends HashMap[String, Any] with Utils {

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

  override def equals(other: Any) = {
    def prepare(x: HashMap[String, _]) = x.asScala
    other match {
      case x: HashMap[String, _] => prepare(x).equals(prepare(x))
      case _ => false
    }
  }

}

case class MetadataOptions {
  val saveMetadata: Boolean = false
}

object FileAttributes extends Utils {

  val posixGroup = "posix:group"
  val posixPermissions = "posix:permissions"
  val owner = "owner"
  val lastModified = "lastModifiedTime"
  val creationTime = "creationTime"

  def apply(path: Path) = {
    val out = new FileAttributes()
    def add(attr: String, o: Object) = o match {
      case ft: FileTime => out.put(attr, ft.toMillis())
      case x: String => out.put(attr, x)
      case p: Principal => out.put(attr, p.getName())
    }

    def readAttributes[T <: BasicFileAttributes](implicit m: Manifest[T]) =
      Files.readAttributes[T](path, m.runtimeClass.asInstanceOf[Class[T]], LinkOption.NOFOLLOW_LINKS)

    val attrs = readAttributes[BasicFileAttributes]

    val keys = List(lastModified, creationTime)
    keys.foreach { k =>
      val m = attrs.getClass().getMethod(k)
      m.setAccessible(true)
      add(k, m.invoke(attrs))
    }

    try {
      val posix = readAttributes[PosixFileAttributes]
      if (posix != null) {
        add(owner, posix.owner())
        add(posixGroup, posix.group())
        add(posixPermissions, PosixFilePermissions.toString(posix.permissions()))
      }
    } catch {
      case e: UnsupportedOperationException => // ignore, not a posix system
    }

    out
  }

  def restore(attrs: FileAttributes, file: File) {
    val path = file.toPath()
    def lookupService = file.toPath().getFileSystem().getUserPrincipalLookupService()
    lazy val posix = Files.getFileAttributeView(path, classOf[PosixFileAttributeView])
    val dosOnes = "hidden,archive,readonly".split(",").toSet
    for ((k, o) <- attrs.asScala) {
      try {
        val name = if (dosOnes.contains(k)) "dos:" + k else k
        val toSet: Option[Any] = (k, o) match {
          case (k, time) if k.endsWith("Time") => Some(FileTime.fromMillis(o.toString.toLong))
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
        }
        toSet.foreach { s =>
          Files.setAttribute(file.toPath(), name, s, LinkOption.NOFOLLOW_LINKS)
        }
      } catch {
        case e: IOException => l.warn("Failed to restore attribute " + k + " for file " + file)
      }
    }
  }

}

trait UpdatePart {
  def size: Long
  def path: String
  @JsonIgnore def name = pathParts.last
  @JsonIgnore def pathParts = path.split("[\\\\/]")
}

case class FileDeleted(path: String) extends UpdatePart {
  def size = 0L
}

object FileDeleted {
  def apply(x: BackupPart) = x match {
    case FolderDescription(path, _) => new FileDeleted(path)
    case FileDescription(path, _, _) => new FileDeleted(path)
  }
}

case class FileDescription(path: String, size: Long, attrs: FileAttributes) extends BackupPart {
  var hash: Array[Byte] = null
  @JsonIgnore def isFolder = false
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

class BackupDescription(val files: Buffer[FileDescription], val folders: Buffer[FolderDescription],
  val symlinks: Buffer[SymbolicLink], val deleted: Buffer[FileDeleted]) {
  def this() = this(Buffer.empty, Buffer.empty, Buffer.empty, Buffer.empty)
  def merge(later: BackupDescription) = {
    val set = later.deleted.map(_.path)
    def remove[T <: BackupPart](x: Buffer[T]) = {
      x.filterNot(bp => set contains bp.path)
    }
    new BackupDescription(remove(files) ++ later.files, remove(folders) ++ later.folders,
      remove(symlinks) ++ later.symlinks, later.deleted)
  }
  
  @JsonIgnore def asMap(): mutable.Map[String, BackupPart] = {
    val map = new mutable.HashMap[String, BackupPart]
    (files ++ folders ++ symlinks).foreach {x => map += ((x.path, x))}
    map
  }
  
  def += (x: UpdatePart) = x match {
    case x: FileDescription => files += x
    case x: FolderDescription => folders += x
    case x: SymbolicLink => symlinks += x
  }
  
  @JsonIgnore def size = files.size + folders.size + symlinks.size
  
  @JsonIgnore def isEmpty() = size == 0
  
}

/**
 * A wrapper for a byte array so it can be used in a map as a key.
 */
class BAWrapper2(ba: Array[Byte]) {
  def data: Array[Byte] = if (ba == null) Array.empty[Byte] else ba
  def equals(other: BAWrapper2): Boolean = Arrays.equals(data, other.data)
  override def equals(obj: Any): Boolean =
    obj match {
      case other: BAWrapper2 => equals(other)
      case _ => false
    }

  override def hashCode: Int = Arrays.hashCode(data)
}

object BAWrapper2 {
  implicit def byteArrayToWrapper(a: Array[Byte]) = new BAWrapper2(a)
}

// Domain classes
case class Size(bytes: Long) {
  override def toString = Utils.readableFileSize(bytes)
}

object Size {
  val knownTypes: Set[Class[_]] = Set(classOf[Size])
  val patt = Pattern.compile("([\\d.]+)[\\s]*([GMK]?B)", Pattern.CASE_INSENSITIVE)

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

case class ZipEntryDescription(filename: String, serializerType: String, objectType: String)
