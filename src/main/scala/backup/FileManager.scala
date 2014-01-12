package backup

import scala.reflect.ClassTag
import scala.collection.mutable.Buffer
import scala.collection.mutable.HashMap
import java.io.File

  class VolumeIndex extends HashMap[String, Int]
  
  class Volume extends HashMap[String, Array[Byte]]
  
  class Parity
  
  case class FileType[T : ClassTag](prefix: String, metadata: Boolean, suffix: String) {
	 var options: BackupFolderOption = null
	 
     var local = true
     var remote = true
     var redundant = false
     var hasDate = false
     
    def this(prefix: String, metadata: Boolean, suffix: String,
    		localC: Boolean = true, remoteC: Boolean = true, redundantC: Boolean=false, hasDateC : Boolean = false) {
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
    
    def nextName = s"$prefix$nextNum$suffix"
    
    def nextFile(f: File = options.backupFolder) = new File(f, nextName)
     
    def matches(x: File) = x.getName().startsWith(prefix)
     
    def getFiles(f: File = options.backupFolder) = f.
    				listFiles().filter(_.isFile()).
    				filter(_.getName().startsWith(prefix))
    				
    def getNum(x: File) = {
      assert(matches(x))
      val num = x.getName().drop(prefix.length()).takeWhile(x => (x+"").matches("\\d"))
      if (num.isEmpty()) {
        -1
      } else
        num.toInt
    }
  }


class FileManager(options: BackupFolderOption) {
  
  val volumes = new FileType[Volume]("volume_", false, ".zip", localC = false)
  val hashchains = new FileType[Buffer[(BAWrapper2, Array[Byte])]]("hashchains_", false, ".obj")
  val files = new FileType[Buffer[BackupPart]]("files_", true, ".obj", hasDateC=true)
  val index = new FileType[VolumeIndex]("index_", true, ".obj", redundantC = true)
  val par2File = new FileType[Parity]("par_", true, ".par2", localC = false, redundantC = true)
  val par2ForVolumes = new FileType[Parity]("par_volume_", true, ".par2", localC = false, redundantC = true)
  val par2ForHashChains = new FileType[Parity]("par_hashchain_", true, ".par2", localC = false, redundantC = true)
  val par2ForFiles = new FileType[Parity]("par_files_", true, ".par2", localC = false, redundantC = true)
  
  private val types = List(volumes, hashchains, files, index, par2File)
  
  types.foreach(_.options = options)
  
  def getFileType(x: File) = types.find(_.matches(x)).get
  
}