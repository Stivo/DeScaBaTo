package backup

import java.io.FileInputStream
import java.io.FileOutputStream
import java.text.SimpleDateFormat
import scala.collection.mutable.Buffer
import java.io.File
import java.util.Date
import java.io.ByteArrayOutputStream

trait BlockStrategy {
  def blockExists(hash: Array[Byte]) : Boolean
  def writeBlock(hash: Array[Byte], buf: Array[Byte])
  def readBlock(hash: Array[Byte]) : Array[Byte]
}

class BackupBaseHandler[T <: BackupFolderOption](val folder: T) extends Utils {
  import Streams._
  import Test._
  import ByteHandling._
  
  val blockStrategy = folder.getBlockStrategy
  
  def loadBackupDescriptions() = {
    val files = folder.backupFolder.listFiles().filter(_.isFile()).filter(_.getName().startsWith("files"))
    val sorted = files.sortBy(_.getName())
    implicit val options = folder
    if (files.size > 0) {
      val lastPattern = sorted.last.getName().takeWhile(_!= '_')
      val filesToLoad = sorted.dropWhile(!_.getName().contains(lastPattern))
      filesToLoad.map(readObject[Buffer[BackupPart]]).reduce(_ ++ _)
    } else
      Buffer[BackupPart]()
  }

  val s = new SimpleDateFormat("yyyy-MM-dd.HHmmss.SSS")
  
}

class FolderBlockStrategy(option: BackupFolderOption) extends BlockStrategy {
  import Streams._
  import Test._
  import ByteHandling._
  val blocksFolder = new File(option.backupFolder, "blocks")
  
  def blockExists(b: Array[Byte]) = {
    new File(blocksFolder, encodeBase64Url(b)).exists()
  }
  def writeBlock(hash: Array[Byte], buf: Array[Byte]) {
    val hashS = encodeBase64Url(hash)
    val f = new File(blocksFolder,hashS)
    val fos = newFileOutputStream(f)(option)
    fos.write(buf)
    fos.close()
  }
  
  val buf = Array.ofDim[Byte](128*1024+10)
  
  def readBlock(x: Array[Byte]) = {
      val out = new ByteArrayOutputStream()
      val fis = newFileInputStream(new File(blocksFolder, encodeBase64Url(x)))(option)
      while (fis.available() > 0) {
    	  val newOffset = fis.read(buf, 0, buf.length - 1)
		  if (newOffset > 0) {
			  out.write(buf, 0, newOffset)
		  }
      }
      fis.close()
      out.toByteArray()
  }

  
}
class BackupHandler(val options: BackupOptions) extends BackupBaseHandler[BackupOptions](options){
  import Streams._
  import Test._
  import ByteHandling._
  
  def backupFolder() {
    new File(options.backupFolder, "blocks").mkdirs()
    l.info("Starting to backup")
  	val oldOnes = loadBackupDescriptions()
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
      writeObject(files, new File(options.backupFolder, filenamenow))(options)
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

  
  def backupFolderDesc(file: File) = {
    val fa = FileAttributes(file)
    new FolderDescription(file.getAbsolutePath(), fa)
  }
  
  def backupFile(file: File, oldOnes: Buffer[BackupPart]) = {
    import blockStrategy._
    def makeNew = {
        l.info(s"File ${file.getName} is new / has changed, backing up")
   	    val fis = new FileInputStream(file)
	    val md = options.getMessageDigest
	    var out = Buffer[Array[Byte]]()
	    
	    val blockHasher = new BlockOutputStream(options.blockSize.bytes.toInt, {
	      buf : Array[Byte] => 
	        
	        val hash = md.digest(buf)
	        out += hash
	        if (!blockExists(hash)) {
	          writeBlock(hash, buf)
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

class RestoreHandler(options: RestoreOptions) extends BackupBaseHandler[RestoreOptions](options) {
  import Streams._
  import Test._
  import ByteHandling._
  
  val relativeTo = options.relativeToFolder.getOrElse(options.restoreToFolder)
      
  def restoreFolder() {
    val filesDb = loadBackupDescriptions()
    val dest = options.restoreToFolder
    val relativeTo = options.relativeToFolder.getOrElse(options.restoreToFolder) 
    val f = new File(options.backupFolder, "files.db")
    val (foldersC, filesC) = filesDb.partition{case f: FolderDescription => true; case _ => false}
    val files = filesC.map(_.asInstanceOf[FileDescription])
    val folders = foldersC.map(_.asInstanceOf[FolderDescription])
    folders.foreach(x => restoreFolderDesc(x))
    files.foreach(restoreFileDesc(_))
    folders.foreach(x => restoreFolderDesc(x))
  }
  
  def restoreFolderDesc(fd: FolderDescription) {
    val restoredFile = new File(options.restoreToFolder,  fd.relativeTo(relativeTo).getPath())
    restoredFile.mkdirs()
    fd.applyAttrsTo(restoredFile)
  }
  
  def restoreFileDesc(fd: FileDescription) {
    val hashes = fd.hashList.grouped(options.getMessageDigest.getDigestLength())
    val restoredFile = new File(options.restoreToFolder, fd.relativeTo(relativeTo).getPath())
    val fos = new FileOutputStream(restoredFile)
    for (x <- hashes) {
      l.trace(s"Restoring from block $x")
      fos.write(blockStrategy.readBlock(x))
    }
    fos.close()
    if (restoredFile.length() != fd.size) {
      l.error(s"Restoring failed for $restoredFile (old size: ${fd.size}, now: ${restoredFile.length()}")
    }
    fd.applyAttrsTo(restoredFile)
  }

}

class SearchHandler(options: FindOptions) extends BackupBaseHandler[FindOptions](options) {
  import ByteHandling._
  def findInBackup(options: FindOptions) {
    l.info("Loading information")
    val oldOnes = loadBackupDescriptions()
    l.info("Information loaded, filtering")
    val filtered = oldOnes.filter(_.path.contains(options.filePattern))
    l.info(s"Filtering done, found ${filtered.size} entries")
    filtered.take(100).foreach(println)
    val size = filtered.map(_.size).fold(0L)(_+_)
    l.info(s"Total ${readableFileSize(size)} found in ${filtered.size} files")
  }
}