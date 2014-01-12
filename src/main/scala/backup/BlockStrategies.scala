package backup

import java.io.FileInputStream
import java.io.FileOutputStream
import java.io.ByteArrayOutputStream
import java.util.Arrays
import java.io.OutputStream
import java.io.File
import java.io.IOException
import scala.concurrent.Future

/**
 * A block strategy saves and retrieves blocks.
 * Blocks are a chunk of a file, keyed with their hash.
 */
trait BlockStrategy {
  def option: BackupFolderOption 
  def blockExists(hash: Array[Byte]) : Boolean
  def writeBlock(hash: Array[Byte], buf: Array[Byte])
  def readBlock(hash: Array[Byte]) : Array[Byte]
  def getBlockSize(hash: Array[Byte]) : Long
  def calculateOverhead(map: Iterable[Array[Byte]]) : Long
  def finishWriting() {}
  def free() {}
}

/**
 * Simple block strategy that dumps all blocks into one folder.
 */
class FolderBlockStrategy(val option: BackupFolderOption) extends BlockStrategy {
  import Streams._
  import Utils._
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
  
  def readBlock(x: Array[Byte]) = {
      val out = new ByteArrayOutputStream()
      val fis = newFileInputStream(new File(blocksFolder, encodeBase64Url(x)))(option)
      copy(fis, out)
      out.toByteArray()
  }
  def getBlockSize(hash: Array[Byte]) = new File(blocksFolder, encodeBase64Url(hash)).length()
  def calculateOverhead(map: Iterable[Array[Byte]]) = {
    map.map(_.grouped(option.hashLength).foldLeft(0L){(x, y) =>
      x + getBlockSize(y)
    }).sum
  }
}

/**
 * A wrapper for a byte array so it can be used in a map as a key.
 */
class BAWrapper2(ba:Array[Byte]) {
  def data: Array[Byte] = if (ba == null) Array.empty[Byte] else ba
  def equals(other:BAWrapper2):Boolean = Arrays.equals(data, other.data)
  override def equals(obj:Any):Boolean = 
    if (obj.isInstanceOf[BAWrapper2]) equals(obj.asInstanceOf[BAWrapper2]) 
    else false
    
  override def hashCode:Int = Arrays.hashCode(data)
}

object BAWrapper2 {
  implicit def byteArrayToWrapper(a: Array[Byte]) = new BAWrapper2(a)
}

/**
 * A block strategy that creates zip files with parts of blocks in it.
 * Additionally for each volume an index is written, which stores the hashes
 * stored in that volume.
 * File patterns:
 * volume_num.zip => A zip file containing blocks with their hash used as filenames.
 * index_num.zip => A file containing all the hashes of the volume with the same number.
 * TODO Not a valid zip file!
 */
class ZipBlockStrategy(val option: BackupFolderOption, volumeSize: Option[Size] = None) extends BlockStrategy with Utils {
  import Streams._
  import Utils._
  import BAWrapper2.byteArrayToWrapper
  import scala.concurrent._
  import scala.concurrent.duration._
  import ExecutionContext.Implicits.global

  
  var setupRan = false
  var knownBlocks: Map[BAWrapper2, Int] = Map()
  var knownBlocksTemp: Map[BAWrapper2, Int] = Map()
  var curNum = 0
  
  implicit val prefix = "index_"
  
  /**
   * Zip file strategy needs a mapping of hashes to volume to work.
   * This has to be set up before any of the functions work, so all the
   * functions call it first.
   */
  def setup() {
    if (setupRan) 
      return
    val index = option.fileManager.index
    val indexes = index.getFiles()
    indexes.foreach { f =>
      val num: Int = index.getNum(f)
      val bos = new BlockOutputStream(option.hashLength, { hash : Array[Byte] =>
        l.trace(s"Adding hash ${encodeBase64Url(hash)} for volume $num")
        knownBlocks += ((hash, num))
      });
      val sis = new SplitInputStream(newFileInputStream(f)(option), bos::Nil)
      sis.readComplete
    }
    curNum = index.nextNum
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
    	Actors.remoteManager ! UploadFile(new File(option.backupFolder, indexName(curNum)), false)
    	Actors.remoteManager ! UploadFile(new File(option.backupFolder, volumeName(curNum)), true)
    	knownBlocks ++= knownBlocksTemp
    	knownBlocksTemp = Map()
    	curNum +=1
    	currentZip = null
    	currentIndex = null
    }
  }
  
  override def finishWriting() {
    finishFutures(true)
    endZip
  }
  
  def finishFutures(force: Boolean = false) {
    while (!futures.isEmpty && (force || futures.head.isCompleted)) {
      Await.result(futures.head, 10 minutes) match {
        case (hash, block) => {
            val hashS = encodeBase64Url(hash)
		    if (currentZip != null && currentZip.size + block.length + hashS.length > volumeSize.get.bytes) {
		      endZip
		    }
		    if (currentZip == null) {
		      startZip
		    }
		    currentZip.writeEntry(hashS, {_.write(block)})
		    currentIndex.write(hash)
	     }
      }
      futures = futures.tail
    }

  }
  
  var currentZip : ZipFileWriter = null
  
  var currentIndex : OutputStream = null
  private var futures : List[Future[(Array[Byte], Array[Byte])]] = List()
  def writeBlock(hash: Array[Byte], buf: Array[Byte]) {
    if (!volumeSize.isDefined) {
      throw new IllegalArgumentException("Volume size needs to be set when writing new volumes")
    }
    setup()
    val hashS = encodeBase64Url(hash)
    if (!knownBlocksTemp.contains(hash)) {
        
        knownBlocksTemp += ((hash, curNum))

        futures :+= future {
          val encrypt = newByteArrayOut(buf)(option)
          (hash, encrypt)
        }
        if (futures.size >= 8) {
          Await.result(futures.head, 10 minutes)
        }
        finishFutures(false)
    } else {
      l.debug(s"File already contains this hash $hashS")
    }
  }

  def getBlockSize(hash: Array[Byte]) = {
    setup()
    val hashS = encodeBase64Url(hash)
    l.trace(s"Getting block for hash $hashS")
    val num: Int = knownBlocks.get(hash).get
    val zipFile = getZipFileReader(num)
    zipFile.getEntrySize(hashS)
  }
  
  def readBlock(hash: Array[Byte]) = {
    setup()
    val hashS = encodeBase64Url(hash)
    l.trace(s"Getting block for hash $hashS")
    val num: Int = knownBlocks.get(hash).get
    val zipFile = getZipFileReader(num)
    if (!zipFile.file.exists()) {
      Actors.downloadFile(zipFile.file)
    }
    val input = zipFile.getStream(hashS)
    readFully(input)(option)
  }

  def calculateOverhead(hashchains: Iterable[Array[Byte]]) = {
    var remain = Map[BAWrapper2, Int]()
    remain ++= knownBlocks
    hashchains.foreach(_.grouped(option.hashLength).foreach(x => remain -= BAWrapper2.byteArrayToWrapper(x)))
    val livingVolumes = remain.values.toSet
    println(livingVolumes.toList.sorted)
    val files = option.fileManager.index.getFiles()
    val deadNow = files.filter(x => !livingVolumes.contains(option.fileManager.index.getNum(x)))
    println(deadNow.mkString(" "))
//    println(remain.toList.filter(_._2==258).mkString(" "))
    deadNow.map(_.length()).sum
  }

  
  var lastZip : Option[(Int, ZipFileReader)] = None
  
  override def free() {
    lastZip.foreach{case (_, zip) => zip.close()}
    lastZip = None
  }
  
  def getZipFileReader(num: Int) = {
    lastZip match {
      case Some((n, zip)) if (n == num) => zip
      case _ => {
        lastZip.foreach{case (_, zip) => zip.close()}
        val out = new ZipFileReader(new File(option.backupFolder, volumeName(num)))
        lastZip = Some((num, out))
        out
      }
    }
  }
  
}
