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
import scala.collection.mutable.HashMap
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

class FileAttributes extends scala.collection.mutable.HashMap[String, Object] {
  
  def hasBeenModified(file: File) : Boolean = {
    val fromMap = get("lastModifiedTime")
    val lastMod = file.lastModified()
    
    val out = fromMap match {
      case Some(ft: FileTime) => ft.toMillis() != lastMod
      case _ => false
    }
    out
  }
}

object FileAttributes {
  def apply(file: File) = {
    val attrs = Files.readAttributes(file.toPath(),"dos:hidden,readonly,archive,creationTime,lastModifiedTime,lastAccessTime");
    val map = attrs.asScala
//	    for ((k,v) <- map) {
//	      println(s"$k $v")
//	    }
    val fa = new FileAttributes()
    fa ++= map
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

} 

trait BackupPart extends Utils {
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
        l.info(files.toString)
        files match {
            case (x::xTail, y:: yTail) if (compare(x,y)) => cutFirst(xTail, yTail)
            case (_, x) => x.mkString("/")
        }
    }
    val cut = cutFirst(files)
    def cleaned(s: String) = if (Test.isWindows) s.replaceAllLiterally(":", "_") else s
    val out = new File(cleaned(cut)).toString
    l.info(s"Relativized $path to $to, got $out")
    out
  }
  
  def applyAttrsTo(f: File) {
    val dosOnes = "hidden,archive,readonly".split(",").toSet
    for ((k, o) <- attrs) {
      val name = if (dosOnes.contains(k)) "dos:"+k else k
      Files.setAttribute(f.toPath(), name, o)
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
    
  def getKryo() = {
    val out = KryoSerializer.registered.newKryo
    out.register(classOf[FileAttributes], new Serializer[FileAttributes](){
      def write (kryo: Kryo, output: Output, fa: FileAttributes){
        kryo.writeObject(output, fa.toList)
      }
      def read (kryo: Kryo, input: Input, clazz: Class[FileAttributes]) = {
        val fa = new FileAttributes()
        val map = kryo.readObject(input, classOf[List[(String, Object)]])
        fa ++= map
        fa
      }
    })
    out.register(classOf[FileTime], new Serializer[FileTime](){
      def write (kryo: Kryo, output: Output, fa: FileTime){
        kryo.writeObject(output, fa.toMillis())
      }
      def read (kryo: Kryo, input: Input, clazz: Class[FileTime]) = {
        val millis = kryo.readObject(input, classOf[Long])
        FileTime.fromMillis(millis)
      }
    })
    out.register(classOf[BAWrapper2], new Serializer[BAWrapper2](){
      def write (kryo: Kryo, output: Output, fa: BAWrapper2){
        kryo.writeObject(output, fa.data)
      }
      def read (kryo: Kryo, input: Input, clazz: Class[BAWrapper2]) = {
        val data = kryo.readObject(input, classOf[Array[Byte]])
        BAWrapper2.byteArrayToWrapper(data)
      }
    })
    out
  }
  
  def writeObject(a: Any, filename: File)(implicit options: FileHandlingOptions) {
    val kryo = getKryo()
    val output = new Output(newFileOutputStream(filename));
    kryo.writeObject(output, a);
    output.close();
  }
  
  def readObject[T](filename: File)(implicit m: Manifest[T], options: FileHandlingOptions) : T = {
    val kryo = getKryo()
    val output = new Input(newFileInputStream(filename));
    kryo.readObject(output, m.erasure).asInstanceOf[T]
  }

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
