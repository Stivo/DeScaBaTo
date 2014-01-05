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

trait Utils extends Logging {
  lazy val l = logger
  def isWindows = System.getProperty("os.name").contains("indows")
} 

trait BackupPart extends Utils {
  def path: String
  def attrs: FileAttributes
  def size : Long
  def relativeTo(to: File) = {
    def lc(u: URI) = 
      if (Test.isWindows) 
        new URI(u.toString().toLowerCase())
      else
        u
    var toUri = lc(to.toURI())
    var relativeUri = lc(new File(path).toURI())
  	var uri = toUri.relativize(relativeUri)
    val regex = """\:""".r;
    if (regex.findAllIn(uri.toString()).size > 1) {
      val s = uri.toString.drop(6)
      uri = new URI("file:/"+s.replace(':', '_'))
    }
  	l.info(s"Relativized $relativeUri to $toUri, got $uri")
  	uri
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
    hash: Array[Byte], hashList: Array[Byte], attrs: FileAttributes) extends BackupPart {
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
  
  val units = Array[String] ( "B", "KB", "MB", "GB", "TB" );
  
  def readableFileSize(size: Long) : String = {
    if(size <= 0) return "0";
    val units = Array[String] ( "B", "KB", "MB", "GB", "TB" );
    val digitGroups = (Math.log10(size)/Math.log10(1024)).toInt;
    return new DecimalFormat("#,##0.#").format(size/Math.pow(1024, digitGroups)) + " " + units(digitGroups);
  }

  object BackupHandler extends Utils {
  
    val blockSize = 1024*1024
        
    def loadBackupDescriptions(backupFolder: File)(implicit options: FileHandlingOptions) = {
      val files = backupFolder.listFiles().filter(_.isFile()).filter(_.getName().startsWith("files"))
      val sorted = files.sortBy(_.getName())
      if (files.size > 0) {
        val lastPattern = sorted.last.getName().takeWhile(_!= '_')
        val filesToLoad = sorted.dropWhile(!_.getName().contains(lastPattern))
        filesToLoad.map(readObject[Buffer[BackupPart]]).reduce(_ ++ _)
      } else
        Buffer[BackupPart]()
    }

    val s = new SimpleDateFormat("yyyy-MM-dd.HHmmss.SSS")
    
    def findInBackup(options: FindOptions) {
      implicit val optionsHere = options
      l.info("Loading information")
      val oldOnes = loadBackupDescriptions(options.backupFolder)
      l.info("Information loaded, filtering")
      val filtered = oldOnes.filter(_.path.contains(options.filePattern))
      l.info(s"Filtering done, found ${filtered.size} entries")
      filtered.take(100).foreach(println)
      val size = filtered.map(_.size).fold(0L)(_+_)
      l.info(s"Total ${readableFileSize(size)} found in ${filtered.size} files")
    }
    
    def backupFolder(options: BackupOptions) {
      implicit val optionsHere = options 
      new File(options.backupFolder, "blocks").mkdirs()
        l.info("Starting to backup")
    	val oldOnes = loadBackupDescriptions(options.backupFolder)
    	l.info(s"Found ${oldOnes.size} previously backed up files")
	    val now = new Date()
	    val filename = s"files-${s.format(now)}"
	    var (counter, fileCounter, sizeCounter) = (0, 0, 0L)
	    val files = Buffer[BackupPart]()

	    def newFiles() {
	      var filenamenow = s"${filename}_$counter.db"
	      counter += 1
	      fileCounter += files.length
	      sizeCounter += files.map(_.size).fold(0L)(_+_)
	      writeObject(files, new File(options.backupFolder, filenamenow))
	      files.clear
	    }
	    def walk(file: File) {
	      if (files.length >= 1000) {
	        newFiles()
	      }
	      file.isDirectory() match {
	        case true => files += backupFolderDesc(file); file.listFiles().foreach(walk)
	        case false => files += backupFile(file, oldOnes)
	      }
	    }
	    walk(options.folderToBackup)
	    newFiles()
	    l.info(s"Backup completed of $fileCounter (${readableFileSize(sizeCounter)})")
	}

    def restoreFolder(args: RestoreOptions) {
        implicit val options = args
        val filesDb = loadBackupDescriptions(args.backupFolder)
        val dest = args.restoreToFolder
        val relativeTo = args.relativeToFolder.getOrElse(args.restoreToFolder) 
	    val f = new File(args.backupFolder, "files.db")
	    val (foldersC, filesC) = filesDb.partition{case f: FolderDescription => true; case _ => false}
	    val files = filesC.map(_.asInstanceOf[FileDescription])
	    val folders = foldersC.map(_.asInstanceOf[FolderDescription])
	    folders.foreach(x => restoreFolderDesc(x, dest, relativeTo))
	    files.foreach(restoreFileDesc(_))
	    folders.foreach(x => restoreFolderDesc(x, dest, relativeTo))
	}
	  
	  def restoreFolderDesc(fd: FolderDescription, dest: File, relativeTo: File = new File("test")) {
	    val restoredFile = new File(dest, fd.relativeTo(relativeTo).getPath())
	    restoredFile.mkdirs()
	    fd.applyAttrsTo(restoredFile)
	  }
	  
	  def restoreFileDesc(fd: FileDescription)(implicit options: RestoreOptions) {
	    val hashes = fd.hashList.grouped(options.getMessageDigest.getDigestLength()).map(encodeBase64Url)
	    val relativeTo = options.relativeToFolder.getOrElse(options.restoreToFolder) 
	    val restoredFile = new File(options.restoreToFolder, fd.relativeTo(relativeTo).getPath())
	    val fos = new FileOutputStream(restoredFile)
	    val buf = Array.ofDim[Byte](blockSize+10)
	    for (x <- hashes) {
	      l.trace(s"Restoring from block $x")
	      val fis = newFileInputStream(new File(options.backupFolder, s"blocks/$x"))
	      while (fis.available() > 0) {
	    	  val newOffset = fis.read(buf, 0, buf.length - 1)
			  if (newOffset > 0) {
				  fos.write(buf, 0, newOffset)
			  }
	      }
	      fis.close()
	    }
	    fos.close()
	    if (restoredFile.length() != fd.size) {
	      l.error(s"Restoring failed for $restoredFile (old size: ${fd.size}, now: ${restoredFile.length()}")
	    }
	    fd.applyAttrsTo(restoredFile)
	  }
	  
	  def backupFolderDesc(file: File)(implicit options: BackupOptions) = {
	    val fa = FileAttributes(file)
	    new FolderDescription(file.getAbsolutePath(), fa)
	  }
	  
	  def backupFile(file: File, oldOnes: Buffer[BackupPart])(implicit options: BackupOptions) = {
	    def makeNew = {
	        l.info(s"File ${file.getName} is new / has changed, backing up")
	   	    val fis = new FileInputStream(file)
		    val md = options.getMessageDigest
		    var out = Buffer[Array[Byte]]()
		    
		    val blockHasher = new BlockOutputStream(blockSize, {
		      buf : Array[Byte] => 
		        
		        val hash = md.digest(buf)
		        val hashS = encodeBase64Url(hash)
		        out += hash
		        val f = new File(options.backupFolder, "blocks/"+hashS)
		        if (!f.exists()) {
		          val fos = newFileOutputStream(f)
		          fos.write(buf)
		          fos.close()
		        }
		    })
		    val hos = new HashingOutputStream(options.getHashAlgorithm)
		    val sis = new SplitInputStream(fis, blockHasher::hos::Nil)
		    sis.readComplete
		    val hashList = out.reduce(_ ++ _)
		    val hash = hos.out.get
		    val fa = FileAttributes(file)
		    new FileDescription(file.getAbsolutePath(), file.length(), hash, hashList, fa)
	    }
	    oldOnes.find(_.path == file.getAbsolutePath()) match {
	      case Some(x: FileDescription) if (!x.attrs.hasBeenModified(file)) =>
	        x
	      case _ => makeNew
	    }
	  }
  
  }
  
  def newFileOutputStream(file: File)(implicit fileHandlingOptions: FileHandlingOptions) : OutputStream = {
    var out: OutputStream = new FileOutputStream(file)
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

  def newFileInputStream(file: File)(implicit fileHandlingOptions: FileHandlingOptions) = {
    var out: InputStream = new FileInputStream(file)
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
    out
  }

}