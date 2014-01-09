package backup

import java.io.FileInputStream
import java.io.FileOutputStream
import java.io.ByteArrayOutputStream
import java.util.Arrays
import java.io.OutputStream
import java.io.File
import java.io.IOException


trait BlockStrategy {
  def blockExists(hash: Array[Byte]) : Boolean
  def writeBlock(hash: Array[Byte], buf: Array[Byte])
  def readBlock(hash: Array[Byte]) : Array[Byte]
  def finishWriting() {}
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

class BAWrapper2(ba:Array[Byte], val length: Int) {
  def data: Array[Byte] = if (ba == null) Array.empty[Byte] else ba
  def equals(other:BAWrapper2):Boolean = Arrays.equals(data, other.data)
  override def equals(obj:Any):Boolean = 
    if (obj.isInstanceOf[BAWrapper2]) equals(obj.asInstanceOf[BAWrapper2]) 
    else false
  override def hashCode:Int = Arrays.hashCode(data)
}

object BAWrapper2 {
  implicit def byteArrayToWrapper(a: Array[Byte]) = new BAWrapper2(a, a.length)
}

trait CountingFileManager {
  def prefix : String
  def getNum(file: File) = file.getName.drop(prefix.length()).takeWhile(x => (x+"").matches("\\d")).toInt
  def getFilesAndNextNum(file: File) = {
    var max : Int = -1
    val indexes = file.listFiles().filter(_.isFile).filter(_.getName.startsWith(prefix)).sortBy(_.getName)
    indexes.map(getNum).foreach { num =>
      max = Math.max(max, num)
    }
    (indexes, max+1)
  }
}

class ZipBlockStrategy(option: BackupFolderOption, volumeSize: Option[Size] = None) extends BlockStrategy with CountingFileManager {
  import Streams._
  import Test._
  import ByteHandling._
  import BAWrapper2.byteArrayToWrapper
  
  var setupRan = false
  var knownBlocks: Map[BAWrapper2, Int] = Map()
  var knownBlocksTemp: Map[BAWrapper2, Int] = Map()
  var curNum = 0
  
  val prefix = "index_"
  
  def setup() {
    if (setupRan) 
      return
    
    val (indexes, max) = getFilesAndNextNum(option.backupFolder)
    indexes.foreach { f =>
      val num: Int = getNum(f)
      val bos = new BlockOutputStream(option.hashLength, { hash : Array[Byte] =>
        l.trace(s"Adding hash ${encodeBase64Url(hash)} for volume $num")
        knownBlocks += ((hash, num))
      });
      val sis = new SplitInputStream(newFileInputStream(f)(option), bos::Nil)
      sis.readComplete
    }
    curNum = max
    l.info(s"Found highest volume is ${max-1}, so starting at $curNum")
    setupRan = true
  }
  
  def blockExists(b: Array[Byte]) = {
    setup()
    knownBlocks.keySet contains b
  }
  
  def volumeName(num: Int, temp: Boolean = false) = {
    val add = if (temp) ".temp" else ""
    s"volume${add}_$num.zip"
  }
  
  def indexName(num: Int, temp: Boolean = false) = {
    val add = if (temp) ".temp" else ""
    s"index${add}_$num.zip"
  }
  
  def startZip {
    l.info(s"Starting volume ${volumeName(curNum)}")
    currentZip = new ZipFileWriter(new File(option.backupFolder, volumeName(curNum, true)))
    currentIndex = newFileOutputStream(new File(option.backupFolder, indexName(curNum, true)))(option)
  }
  
  def endZip {
    if (currentZip != null) {
        l.info(s"Ending zip file $curNum")
    	currentZip.close()
    	currentIndex.close()
    	def rename(f : Function2[Int, Boolean, String]) {
        	new File(option.backupFolder, f(curNum, true))
        		.renameTo(new File(option.backupFolder, f(curNum, false)))
        }
    	rename(volumeName)
    	rename(indexName)
    	knownBlocks ++= knownBlocksTemp
    	knownBlocksTemp = Map()
    	curNum +=1
    	currentZip = null
    	currentIndex = null
    }
  }

  
  var currentZip : ZipFileWriter = null
  
  var currentIndex : OutputStream = null
  
  def writeBlock(hash: Array[Byte], buf: Array[Byte]) {
    if (!volumeSize.isDefined) {
      throw new IllegalArgumentException("Volume size needs to be set when writing new volumes")
    }
    setup()
    val hashS = encodeBase64Url(hash)
    if (currentZip != null && currentZip.size + buf.length + hashS.length > volumeSize.get.bytes) {
      endZip
    }
    if (currentZip == null) {
      startZip
    }
    if (!knownBlocksTemp.contains(hash)) {
    	l.trace(s"Writing hash to $curNum")
       currentZip.writeEntry(hashS, newByteArrayOut(buf)(option))
       currentIndex.write(hash)
       knownBlocksTemp += ((hash, curNum))
    } else {
      l.debug(s"File already contains this hash $hashS")
    }
  }
  
  val buf = Array.ofDim[Byte](128*1024+10)
  
  var lastZip : Option[(Int, ZipFileReader)] = None
  	
  def getZipFileReader(num: Int) = {
    lastZip match {
      case Some((n, zip)) if (n == num) => zip
      case _ => {
        val out = new ZipFileReader(new File(option.backupFolder, volumeName(num)))
        lastZip = Some((num, out))
        out
      }
    }
  }
  
  def readBlock(hash: Array[Byte]) = {
    setup()
    val hashS = encodeBase64Url(hash)
    l.trace(s"Getting block for hash $hashS")
    val num: Int = knownBlocks.get(hash).get
    val zipFile = getZipFileReader(num)
    val input = zipFile.getStream(hashS)
    readFully(input)(option)
  }
  
  override def finishWriting() {
    endZip
  }
}
