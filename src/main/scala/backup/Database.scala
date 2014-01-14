package backup;

import scala.collection.JavaConverters._
import javax.xml.bind.DatatypeConverter
import java.text.DecimalFormat
import java.util.HashMap
import java.nio.file.Files
import java.io.File
import com.typesafe.scalalogging.slf4j.Logging
import java.nio.file.attribute.FileTime

class FileAttributes extends HashMap[String, Any] {

  def hasBeenModified(file: File): Boolean = {
    val fromMap = get("lastModifiedTime")
    val lastMod = file.lastModified()

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

object FileAttributes {
  def apply(file: File, options: BackupOptions) = {
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

object Utils {
  implicit def string2Size(s: String) = SizeParser.parse(s)

  private val units = Array[String]("B", "KB", "MB", "GB", "TB");
  def isWindows = System.getProperty("os.name").contains("indows")
  def readableFileSize(size: Long, afterDot: Int = 1): String = {
    if (size <= 0) return "0";
    val digitGroups = (Math.log10(size) / Math.log10(1024)).toInt;
    val afterDotPart = if (afterDot == 0) "#" else "0" * afterDot
    return new DecimalFormat("#,##0." + afterDotPart).format(size / Math.pow(1024, digitGroups)) + " " + Utils.units(digitGroups);
  }

  def encodeBase64(bytes: Array[Byte]) = DatatypeConverter.printBase64Binary(bytes);
  def decodeBase64(s: String) = DatatypeConverter.parseBase64Binary(s);

  def encodeBase64Url(bytes: Array[Byte]) = encodeBase64(bytes).replace('+', '-').replace('/', '_')
  def decodeBase64Url(s: String) = decodeBase64(s.replace('-', '+').replace('_', '/'));

}

trait Utils extends Logging {
  lazy val l = logger
  import Utils._

  def readableFileSize(size: Long): String = Utils.readableFileSize(size)

  def printDeleted(message: String) {
    ConsoleManager.writeDeleteLine(message)
  }

}

trait UpdatePart

case class FileDeleted(val path: String, val folder: Boolean) extends UpdatePart

object FileDeleted {
  def apply(x: BackupPart) = x match {
    case FolderDescription(path, _) => new FileDeleted(path, true)
    case FileDescription(path, _, _, _, _) => new FileDeleted(path, false)
  }
}

trait BackupPart extends UpdatePart {
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

case class FileDescription(path: String, size: Long,
  hash: Array[Byte], hashChain: Array[Byte], attrs: FileAttributes) extends BackupPart {
  def name = path.split("""[/\\]""").last
}

case class FolderDescription(path: String, attrs: FileAttributes) extends BackupPart {
  val size = 0L
}

