package ch.descabato

import java.io.File
import java.util.HashMap
import scala.collection.JavaConverters._
import java.nio.file.Files
import java.nio.file.attribute.FileTime
import java.util.Arrays
import java.nio.file.attribute.BasicFileAttributes
import com.fasterxml.jackson.annotation.JsonIgnore
import java.util.regex.Pattern
import java.math.{BigDecimal => JBigDecimal}
import java.nio.file.LinkOption

class FileAttributes extends HashMap[String, Any] with Utils {

  def hasBeenModified(file: File): Boolean = {
    val fromMap = get("lastModifiedTime")
    val lastMod = {
      Files.getAttribute(file.toPath(), "lastModifiedTime", LinkOption.NOFOLLOW_LINKS) match {
        case x: FileTime => x.toMillis()
        case _ => l.warn("Did not find filetime for "+file); 0L
      }
    }

    val out = Option(fromMap) match {
      case Some(l: Long) => !(lastMod <= l && l <= lastMod)
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

object FileAttributes {

  def convert(attrs: BasicFileAttributes) = {
    var fa = new FileAttributes()

    val keys = List("lastModifiedTime", "creationTime")
    keys.foreach { k =>
      val m = attrs.getClass().getMethod(k)
      m.setAccessible(true)
      add(k, m.invoke(attrs))
    }

    def add(attr: String, o: Object) = o match {
      case ft: FileTime => fa.put(attr, ft.toMillis())
    }

    fa
  }

  def apply(file: File, options: MetadataOptions) = {
    val attrs = if (options.saveMetadata) {
      Files.readAttributes(file.toPath(), "dos:hidden,readonly,archive,creationTime,lastModifiedTime,lastAccessTime");
    } else {
      val hm = new HashMap[String, Object]()
      hm.put("lastModifiedTime", file.lastModified().asInstanceOf[java.lang.Long]);
      hm
    }
    val map = attrs.asScala.map {
      // Lose some precision, millis is enough
      case (k, ft: FileTime) => (k, ft.toMillis())
      case (k, ft: java.lang.Long) => (k, ft)
      case (k, ft) if (k.endsWith("Time")) =>
        (k, ft.toString.toLong)
      case x => x
    }
    //	    for ((k,v) <- map) {
    //	      println(s"$k $v")
    //	    }
    var fa = new FileAttributes()
    map.foreach { case (k, v) => fa.put(k, v) }
    fa
  }

}

trait UpdatePart {
  def size : Long
}

case class FileDeleted(val path: String) extends UpdatePart {
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
  def name = pathParts.last
  @JsonIgnore def isFolder = false
}

case class FolderDescription(path: String, attrs: FileAttributes) extends BackupPart {
  @JsonIgnore val size = 0L
  @JsonIgnore def isFolder = true
}

trait BackupPart extends UpdatePart {
  @JsonIgnore def isFolder: Boolean
  @JsonIgnore def pathParts = path.split("[\\\\/]")
  def path: String
  def attrs: FileAttributes
  def size: Long

  def relativeTo(to: File) = {
    // Needs to take common parts out of the path.
    // Different semantics on windows. Upper-/lowercase is ignored, ':' may not be part of the output
    def prepare(f: File) = {
      var path = f.getAbsolutePath
      if (Utils.isWindows)
        path = path.replaceAllLiterally("\\", "/")
      path.split("/").toList
    }
    val files = (prepare(to), prepare(new File(path)))

    def compare(s1: String, s2: String) = if (Utils.isWindows) s1.equalsIgnoreCase(s2) else s1 == s2

    def cutFirst(files: (List[String], List[String])): String = {
      files match {
        case (x :: xTail, y :: yTail) if (compare(x, y)) => cutFirst(xTail, yTail)
        case (_, x) => x.mkString("/")
      }
    }
    val cut = cutFirst(files)
    def cleaned(s: String) = if (Utils.isWindows) s.replaceAllLiterally(":", "_") else s
    val out = new File(cleaned(cut)).toString
    out
  }

  def applyAttrsTo(f: File) {
    val dosOnes = "hidden,archive,readonly".split(",").toSet
    for ((k, o) <- attrs.asScala) {
      val name = if (dosOnes.contains(k)) "dos:" + k else k
      val toSet = if (k.endsWith("Time")) {
        FileTime.fromMillis(o.toString.toLong)
      } else
        o
      Files.setAttribute(f.toPath(), name, toSet)
    }
  }

}

case class SymbolicLink(val path: String, val linkTarget: String, val attrs: FileAttributes) extends BackupPart {
  def size = 0L
  def isFolder = false
}

/**
 * A wrapper for a byte array so it can be used in a map as a key.
 */
class BAWrapper2(ba: Array[Byte]) {
  def data: Array[Byte] = if (ba == null) Array.empty[Byte] else ba
  def equals(other: BAWrapper2): Boolean = Arrays.equals(data, other.data)
  override def equals(obj: Any): Boolean =
    if (obj.isInstanceOf[BAWrapper2]) equals(obj.asInstanceOf[BAWrapper2])
    else false

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
    var out: Long = -1;
    val matcher = patt.matcher(size);
    val map = List(("GB", 3), ("MB", 2), ("KB", 1), ("B", 0)).toMap
    if (matcher.find()) {
      val number = matcher.group(1);
      val pow = map.get(matcher.group(2).toUpperCase()).get;
      var bytes = new BigDecimal(new JBigDecimal(number));
      bytes = bytes.*(BigDecimal.valueOf(1024).pow(pow));
      out = bytes.longValue();
    }
    new Size(out);
  }
}
