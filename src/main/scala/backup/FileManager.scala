package backup

import scala.reflect.ClassTag
import scala.collection.mutable.Buffer
import scala.collection.mutable.HashMap
import java.io.File
import java.text.SimpleDateFormat
import java.util.Date

  class VolumeIndex extends HashMap[String, Int]
  
  class Volume extends HashMap[String, Array[Byte]]
  
  class Parity
  
  case class FileType[T](prefix: String, metadata: Boolean, suffix: String)(implicit val m: Manifest[T]) extends Utils {
     
	 var options: BackupFolderOption = null
	 var fileManager: FileManager = null
	 
     var local = true
     var remote = true
     var redundant = false
     var hasDate = false
     
    def this(prefix: String, metadata: Boolean, suffix: String,
    		localC: Boolean = true, remoteC: Boolean = true, redundantC: Boolean=false, hasDateC : Boolean = false)(implicit m: Manifest[T]) {
    	this(prefix, metadata, suffix)
    	local = localC
    	remote = remoteC
    	redundant = redundantC
    	hasDate = hasDateC
    }
     
    def nextNum = {
      val col = getFiles().map(getNum)
      if (col.isEmpty) {
        0
      } else {
        col.max + 1
      }
    }
    
    def nextName = {
      val date = if (hasDate) fileManager.dateFormat.format(fileManager.startDate)+"_" else "" 
      s"$prefix$date$nextNum$suffix"
    }
    
    def nextFile(f: File = options.backupFolder) = new File(f, nextName)
     
    def matches(x: File) = x.getName().startsWith(prefix)
     
    def getFiles(f: File = options.backupFolder) = f.
    				listFiles().filter(_.isFile()).
    				filter(_.getName().startsWith(prefix))
    
    def getDate(x: File) = {
      val date = x.getName.drop(prefix.length()).take(fileManager.dateFormatLength)
      fileManager.dateFormat.parse(date)
    }
      
    def getNum(x: File) = {
      assert(matches(x), s"$x does not match $prefix")
      var dropLength = prefix.length
      if (hasDate)
        dropLength += fileManager.dateFormatLength + 1
      val num = x.getName().drop(dropLength).takeWhile(x => (x+"").matches("\\d"))
      if (num.isEmpty()) {
        -1
      } else
        num.toInt
    }
    
    def write(x: T) = {
      val file = nextFile()
      //l.info("Writing "+x+" to file "+file)
      options.serialization.writeObject(x, file)(options, m)
      file
    }
    
  }


class FileManager(options: BackupFolderOption) {
  val dateFormat = new SimpleDateFormat("yyyy-MM-dd.HHmmss.SSS")
  val dateFormatLength = dateFormat.format(new Date()).length() 
  val startDate = new Date()
  
  val volumes = new FileType[Volume]("volume_", false, ".zip", localC = false)
  val hashchains = new FileType[Buffer[(BAWrapper2, Array[Byte])]]("hashchains_", false, ".obj")
  val files = new FileType[Buffer[BackupPart]]("files_", true, ".obj", hasDateC=true)
  val filesDelta = new FileType[Buffer[UpdatePart]]("filesdelta_", true, ".obj", hasDateC=true)
  val index = new FileType[VolumeIndex]("index_", true, ".obj", redundantC = true)
  val par2File = new FileType[Parity]("par_", true, ".par2", localC = false, redundantC = true)
  val par2ForVolumes = new FileType[Parity]("par_volume_", true, ".par2", localC = false, redundantC = true)
  val par2ForHashChains = new FileType[Parity]("par_hashchain_", true, ".par2", localC = false, redundantC = true)
  val par2ForFiles = new FileType[Parity]("par_files_", true, ".par2", localC = false, redundantC = true)
  val par2ForFilesDelta = new FileType[Parity]("par_filesdelta_", true, ".par2", localC = false, redundantC = true)
  
  private val types = List(volumes, hashchains, files, filesDelta, index, par2ForFiles, par2ForVolumes, par2ForHashChains, par2ForFilesDelta, par2File)
  
  types.foreach{ x=> x.options = options; x.fileManager = this}
  
  def getFileType(x: File) = types.find(_.matches(x)).get
  
  def getBackupAndUpdates : (Array[File], Boolean) = {
    val filesNow = options.backupFolder.listFiles().filter(_.isFile()).filter(files.matches)
    val sorted = filesNow.sortBy(_.getName())
    if (filesNow.isEmpty) {
      (Array(), false)
    } else {
      val lastDate = files.getDate(sorted.last)
      val onlyLast = sorted.dropWhile(x => files.getDate(x).before(lastDate))
      val updates = filesDelta.getFiles().dropWhile(filesDelta.getDate(_).before(lastDate))
      (onlyLast ++ updates, !updates.isEmpty)
    }
  }
  
}