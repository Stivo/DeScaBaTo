package backup;

import java.io.File
import java.security.MessageDigest
import java.io.BufferedInputStream
import java.io.FileInputStream
import scala.collection.mutable.Buffer
import scala.annotation.tailrec
import java.nio.file.Path
import java.io.FileOutputStream
import java.io.BufferedOutputStream
import java.io.OutputStream
import javax.xml.bind.DatatypeConverter
import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.Output
import java.nio.file.Files
import com.esotericsoftware.kryo.io.Input
import scala.io.Source
import java.io.PipedOutputStream
import java.util.zip.GZIPOutputStream
import java.util.zip.GZIPInputStream
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.attribute.UserDefinedFileAttributeView
import scala.collection.JavaConverters._
import java.util.GregorianCalendar
import com.twitter.chill.ScalaKryoInstantiator
import com.twitter.chill.KryoSerializer
import com.esotericsoftware.kryo.Serializer
import java.io.IOException
import java.net.URI
import com.typesafe.scalalogging.slf4j.Logging
import java.util.Date
import java.text.SimpleDateFormat
import java.nio.file.attribute.FileTime
import java.text.DecimalFormat
import java.io.InputStream
import java.io.ByteArrayOutputStream
import java.io.ByteArrayInputStream
import scala.collection.mutable.Map
import java.util.HashMap
import com.fasterxml.jackson.databind.JsonSerializable
import scala.collection.convert.decorateAsScala

class FileAttributes extends HashMap[String, Any] {
  
  def hasBeenModified(file: File) : Boolean = {
    val fromMap = get("lastModifiedTime")
    val lastMod = file.lastModified()
    
    val out = fromMap match {
      case Some(ft: Long) => ft != lastMod
      case _ => false
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
  def apply(file: File) = {
    val attrs = Files.readAttributes(file.toPath(),"dos:hidden,readonly,archive,creationTime,lastModifiedTime,lastAccessTime");
    val map = attrs.asScala.map {
      // Lose some precision, millis is enough
      case (k, ft: FileTime) => (k, ft.toMillis())
      case (k, ft) if (k.endsWith("Time")) => 
        (k, FileTime.fromMillis(ft.toString.toLong))
      case x => x
    }
//	    for ((k,v) <- map) {
//	      println(s"$k $v")
//	    }
    var fa = new FileAttributes()
    map.foreach{case (k,v) => fa.put(k,v)}
    fa
  }

}

object Utils {
  val units = Array[String] ( "B", "KB", "MB", "GB", "TB" );
}

trait Utils extends Logging {
  lazy val l = logger
  
  def isWindows = System.getProperty("os.name").contains("indows")

  def readableFileSize(size: Long) : String = {
    if(size <= 0) return "0";
    val digitGroups = (Math.log10(size)/Math.log10(1024)).toInt;
    return new DecimalFormat("#,##0.#").format(size/Math.pow(1024, digitGroups)) + " " + Utils.units(digitGroups);
  }
  
  var lastOne = 0L
  
  def printDeleted(message: String) = {
    val timeNow = new Date().getTime()
    if (timeNow > lastOne) {
    	ConsoleManager.appender.writeDeleteLine(message)
    	lastOne = timeNow + 10
    }
  }
  
} 

trait BackupPart {
  def path: String
  def attrs: FileAttributes
  def size : Long
  
  def relativeTo(to: File) = {
    // Needs to take common parts out of the path.
    // Different semantics on windows. Upper-/lowercase is ignored, ':' may not be part of the output
    def prepare(f: File) = {
        var path = f.getAbsolutePath
        if (Test.isWindows) 
            path = path.replaceAllLiterally("\\", "/")
        path.split("/").toList
    }
    val files = (prepare(to), prepare(new File(path)))
    
    def compare(s1: String, s2: String) = if (Test.isWindows) s1.equalsIgnoreCase(s2) else s1 == s2    
    
    def cutFirst(files: (List[String], List[String])) : String = {
        files match {
            case (x::xTail, y:: yTail) if (compare(x,y)) => cutFirst(xTail, yTail)
            case (_, x) => x.mkString("/")
        }
    }
    val cut = cutFirst(files)
    def cleaned(s: String) = if (Test.isWindows) s.replaceAllLiterally(":", "_") else s
    val out = new File(cleaned(cut)).toString
    out
  }
  
  def applyAttrsTo(f: File) {
    val dosOnes = "hidden,archive,readonly".split(",").toSet
    for ((k, o) <- attrs.asScala) {
      val name = if (dosOnes.contains(k)) "dos:"+k else k
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

object Test extends Utils {
  import ByteHandling._
  import Streams._

  def deleteAll(f: File) {
    def walk(f: File) {
      f.isDirectory() match {
        case true => f.listFiles().foreach(walk); f.delete()
        case false => f.delete()
      }
    }
    walk(f)
  }
  
  def checkCompression() {
    val backupDestinationFolder = new File("backups")
    implicit val options = new FileHandlingOptions() {}
    for (f <- backupDestinationFolder.listFiles().filter(_.isFile())) {
      
    val fis = newFileInputStream(f)
    var count = 0
    while (fis.read() >= 0) {
      count += 1
    }
    fis.close()
    l.info(s"${f.getName} saved: ${f.length} compressed, $count uncompressed")
    }
  }
  
}


object ByteHandling {
  
  def encodeBase64(bytes: Array[Byte]) = DatatypeConverter.printBase64Binary(bytes);
  def decodeBase64(s: String) = DatatypeConverter.parseBase64Binary(s);
  
  def encodeBase64Url(bytes: Array[Byte]) = encodeBase64(bytes).replace('+', '-').replace('/', '_')
  def decodeBase64Url(s: String) = decodeBase64(s.replace('-', '+').replace('_', '/'));
    
  def wrapOutputStream(stream: OutputStream)(implicit fileHandlingOptions: FileHandlingOptions) : OutputStream = {
    var out = stream
    if (fileHandlingOptions.passphrase != null) {
      if (fileHandlingOptions.algorithm == "AES") {
        out = AES.wrapStreamWithEncryption(out, fileHandlingOptions.passphrase, fileHandlingOptions.keyLength)
      } else {
        throw new IllegalArgumentException(s"Unknown encryption algorithm ${fileHandlingOptions.algorithm}")
      }
    }
    if (fileHandlingOptions.compression == CompressionMode.zip) {
      out = new GZIPOutputStream(out)
    }
    out
  }
  
  def newFileOutputStream(file: File)(implicit fileHandlingOptions: FileHandlingOptions) : OutputStream = {
    var out: OutputStream = new FileOutputStream(file)
    wrapOutputStream(out)
  }

  def newByteArrayOut(content: Array[Byte])(implicit fileHandlingOptions: FileHandlingOptions) = {
    var out = new ByteArrayOutputStream()
    val wrapped = wrapOutputStream(out)
    wrapped.write(content)
    wrapped.close()
    val r = out.toByteArray
    r
  }
  
  def readFully(in: InputStream)(implicit fileHandlingOptions: FileHandlingOptions) = {
    val baos = new ByteArrayOutputStream(10240)
    copy(wrapInputStream(in), baos)
    baos.toByteArray()
  }

  def wrapInputStream(in: InputStream)(implicit fileHandlingOptions: FileHandlingOptions) = {
    var out = in
    if (fileHandlingOptions.passphrase != null) {
      if (fileHandlingOptions.algorithm == "AES") {
        out = AES.wrapStreamWithDecryption(out, fileHandlingOptions.passphrase, fileHandlingOptions.keyLength)
      } else {
        throw new IllegalArgumentException(s"Unknown encryption algorithm ${fileHandlingOptions.algorithm}")
      }
    }
    if (fileHandlingOptions.compression == CompressionMode.zip) {
      out = new GZIPInputStream(out)
    }
    out
  }
  
  def newFileInputStream(file: File)(implicit fileHandlingOptions: FileHandlingOptions) = {
    var out: InputStream = new FileInputStream(file)
    wrapInputStream(out)
  }

  def readFrom(in: InputStream, f: (Array[Byte], Int) => Unit) {
    val buf = Array.ofDim[Byte](10240)
    var lastRead = 1
    while (lastRead > 0) {
      lastRead = in.read(buf)
      if (lastRead > 0) {
        f(buf, lastRead)
      }
    }
    in.close()
  }
  
  def copy(in: InputStream, out: OutputStream) {
    readFrom(in, { (x: Array[Byte], len: Int) =>
      out.write(x, 0, len)
    })
    in.close()
    out.close()
  }
  
}
