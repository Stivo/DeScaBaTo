package ch.descabato.core.model

import java.io.File
import java.io.IOException
import java.math.{BigDecimal => JBigDecimal}
import java.nio.file.attribute._
import java.nio.file.Files
import java.nio.file.LinkOption
import java.nio.file.Path
import java.security.Principal
import java.util
import java.util.regex.Pattern

import ch.descabato.core.util.JacksonAnnotations.JsonIgnore
import ch.descabato.utils._

import scala.collection.JavaConverters._

class FileAttributes extends util.HashMap[String, Any] with Utils {

  def lastModifiedTime = get(FileAttributes.lastModified)

  def hasBeenModified(file: File): Boolean = {
    val fromMap = lastModifiedTime
    val lastMod: Long = {
      Files.getAttribute(file.toPath(), FileAttributes.lastModified, LinkOption.NOFOLLOW_LINKS) match {
        case x: FileTime => x.toMillis()
        case _ => l.warn("Did not find filetime for " + file); 0L
      }
    }

    val out = Option(fromMap) match {
      case Some(l: Long) => lastMod != l
      case Some(l: Int) => lastMod != l.toLong
      case Some(ft: String) =>
        val l = ft.toLong; lastMod != l
      case None => true
      case x => println("Was modified: " + lastMod + " vs " + x + " " + x.getClass + " "); true
    }
    out
  }

}

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

    add(lastModified, attrs.lastModifiedTime())
    add(creationTime, attrs.creationTime())
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
      case _: UnsupportedOperationException => // ignore, not a posix system
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
      //      if (dos.isArchive) {
      //        add("dos:archive", true)
      //      }
      //      if (dos.isSystem) {
      //        add("dos:system", true)
      //      }
    } catch {
      case _: UnsupportedOperationException => // ignore, not a dos system
    }
  }

  def restore(attrs: FileAttributes, file: File) {
    val path = file.toPath()

    def lookupService = file.toPath().getFileSystem().getUserPrincipalLookupService()

    lazy val posix = Files.getFileAttributeView(path, classOf[PosixFileAttributeView])
    for ((name, o) <- attrs.asScala) {
      setAttribute(file, path, lookupService, posix, name, o)
    }
    val value = attrs.lastModifiedTime
    if (value != null) {
      setAttribute(file, path, lookupService, posix, FileAttributes.lastModified, value)
    }
  }

  private def setAttribute(file: File, path: Path, lookupService: UserPrincipalLookupService, posix: => PosixFileAttributeView, name: String, o: Any) = {
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
    case FileDescription(path, _, _) => new FileDeleted(path)
  }
}

case class FileDescription(path: String, size: Long, attrs: FileAttributes) extends BackupPart {

  def this(file: File) = {
    this(file.getAbsolutePath, file.length(), FileAttributes(file.toPath))
  }

  @JsonIgnore def isFolder = false

}

object FolderDescription {
  def apply(dir: File): FolderDescription = {
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
      val pow = map(matcher.group(2).toUpperCase())
      var bytes = new BigDecimal(new JBigDecimal(number))
      bytes = bytes.*(BigDecimal.valueOf(1024).pow(pow))
      out = bytes.longValue
    }
    new Size(out)
  }
}
