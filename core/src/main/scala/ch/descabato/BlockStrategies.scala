package ch.descabato

import java.io.FileInputStream
import java.io.ByteArrayOutputStream
import java.util.Arrays
import java.io.OutputStream
import java.io.File
import java.io.IOException
import scala.concurrent.Future
import java.io.InputStream
import java.nio.file.Files
import net.java.truevfs.access.TFile
import net.java.truevfs.access.TFileOutputStream
import java.io.BufferedOutputStream
import net.java.truevfs.access.TVFS
import net.java.truevfs.access.TFileInputStream

/**
 * A block strategy saves and retrieves blocks.
 * Blocks are a chunk of a file, keyed with their hash.
 */
trait BlockStrategy {
  def config: BackupFolderConfiguration
  /**
   * Returns true if and only if the block has been persisted in a non temporary way.
   */
  def blockExists(hash: Array[Byte]): Boolean
  def writeBlock(hash: Array[Byte], buf: Array[Byte], disableCompression: Boolean = false)
  def readBlock(hash: Array[Byte], verifyHash: Boolean = false): InputStream
  def getBlockSize(hash: Array[Byte]): Long
  def calculateOverhead(map: Iterable[Array[Byte]]): Long
  def finishWriting() {}
  def finishReading() {}
  def verify(problemCounter: Counter) {}
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
trait ZipBlockStrategy extends BlockStrategy with Utils {
  import Streams._
  import Utils._
  import BAWrapper2.byteArrayToWrapper
  import scala.concurrent._
  import scala.concurrent.duration._
  import ExecutionContext.Implicits.global

  val fileManager: FileManager
  val volumeSize = config.volumeSize

  var setupRan = false
  var knownBlocks: Map[BAWrapper2, Int] = Map()
  var knownBlocksTemp: Map[BAWrapper2, Int] = Map()
  var curNum = 0

  def deleteTempFiles() {
    def deleteFile(x: File) = {
      l.debug("Deleting temporary file " + x)
      x.delete
    }

    def deleteFiles(prefix: String, ft: FileType[_]) = {
      val files = config.folder.listFiles().filter(_.getName.startsWith(prefix))
      val nums = files.map(f => ft.getNum(f)).toSet
      files.foreach(deleteFile)
      nums
    }
    val set1 = deleteFiles(config.prefix + "temp.index_", fileManager.index)
    val set2 = deleteFiles(config.prefix + "temp.volume_", fileManager.volumes)
    val set = set1 union set2
    fileManager.index.getFiles().filter(x => set contains (fileManager.index.getNum(x))).foreach(deleteFile)
    fileManager.volumes.getFiles().filter(x => set contains (fileManager.volumes.getNum(x))).foreach(deleteFile)
  }

  override def verify(problemCounter: Counter) {
    setup(true, problemCounter)
  }

  /**
   * Zip file strategy needs a mapping of hashes to volume to work.
   * This has to be set up before any of the functions work, so all the
   * functions call it first.
   */
  private def setup(verify: Boolean = false, counter: Counter = null) {
    if (setupRan)
      return
    deleteTempFiles()

    val index = fileManager.index
    val indexes = index.getFiles()
    var lastSet = Set[String]()
    indexes.foreach { f =>

      val num: Int = index.getNum(f)
      val bos = new BlockOutputStream(config.hashLength, { hash: Array[Byte] =>
        l.trace(s"Adding hash ${encodeBase64Url(hash)} for volume $num")
        if (verify) lastSet += encodeBase64Url(hash)
        knownBlocks += ((hash, num))
      });
      val tfile = new TFile(f, "index")
      val fis = new TFileInputStream(tfile)
      val sis = new SplitInputStream(fis, bos :: Nil)
      sis.readComplete
      Utils.closeTFile(tfile)
      if (verify) {
        val zip = getZipFileReader(num)
        for (name <- zip.names) {
          if (!(lastSet contains (name))) {
            if (counter != null) {
              counter += 1
            }
            l.warn("Index file is broken, does not contain " + name)
          }
          lastSet -= name
        }
        for (hash <- lastSet) {
          if (counter != null) {
            counter += 1
          }
          l.warn("Hash " + hash + " is in index, but not in volume")
        }
        lastSet = Set.empty
      }
    }
    curNum = index.nextNum()
    setupRan = true
  }

  def blockExists(b: Array[Byte]) = {
    setup()
    knownBlocks contains b
  }

  def volumeName(num: Int, temp: Boolean = false) = {
    val add = if (temp) Constants.tempPrefix else ""
    s"${config.prefix}${add}volume_$num.zip${config.raes}"
  }

  def indexName(num: Int, temp: Boolean = false) = {
    val add = if (temp) Constants.tempPrefix else ""
    s"${config.prefix}${add}index_$num.zip${config.raes}"
  }

  class IndexWriter(x: File) {
    val file = new TFile(x)
    val index = new TFile(file, "index")
    val fos = new TFileOutputStream(index, true)
    val bos = new BufferedOutputStream(fos)
    def close() {
      bos.close()
      TVFS.umount(file)
    }
  }
  
  def startZip() {
    l.info(s"Starting volume ${volumeName(curNum)}")
    currentZip = new ZipFileWriter(new File(config.folder, volumeName(curNum, true)))
    currentIndex = new IndexWriter(new File(config.folder, indexName(curNum, true)))
  }

  def endZip() {
    if (currentZip != null) {
      l.info(s"Ending zip file $curNum")
      currentZip.close()
      currentIndex.close()
      def rename(f: Function2[Int, Boolean, String]) {
        val from = new File(config.folder, f(curNum, true))
        val to = new File(config.folder, f(curNum, false))
        Files.move(from.toPath(), to.toPath())
        val success = to.exists()
        if (!success)
          l.warn("Could not rename file from " + from + " to " + to)
      }

      rename(volumeName)
      rename(indexName)
      //      Actors.remoteManager ! UploadFile(new File(option.folder, indexName(curNum)), false)
      //      Actors.remoteManager ! UploadFile(new File(option.folder, volumeName(curNum)), true)
      knownBlocks ++= knownBlocksTemp
      knownBlocksTemp = Map()
      curNum += 1
      currentZip = null
      currentIndex = null
    }
  }

  override def finishWriting() {
    finishFutures(true)
    endZip
  }

  private def writeProcessedBlock(hash: Array[Byte], block: Array[Byte]) {
    val hashS = encodeBase64Url(hash)
    if (currentZip != null && currentZip.size + block.length + hashS.length > volumeSize.bytes) {
      endZip
    }
    if (currentZip == null) {
      startZip
    }
    currentZip.writeEntry(hashS, { _.write(block) })
    currentIndex.bos.write(hash)
  }

  def finishFutures(force: Boolean = false) {
    while (!futures.isEmpty && (force || futures.head.isCompleted)) {
      Await.result(futures.head, 10 minutes) match {
        case (hash, block) => {
          writeProcessedBlock(hash, block)
        }
      }
      futures = futures.tail
    }

  }

  var currentZip: ZipFileWriter = null

  var currentIndex: IndexWriter = null
  
  private var futures: List[Future[(Array[Byte], Array[Byte])]] = List()
  def writeBlock(hash: Array[Byte], buf: Array[Byte], disableCompression: Boolean = false) {
    setup()
    val hashS = encodeBase64Url(hash)
    if (!knownBlocksTemp.contains(hash)) {

      knownBlocksTemp += ((hash, curNum))
      val f = () => {
        val encrypt = buf //StreamHeaders.newByteArrayOut(buf, config, disableCompression)
        (hash, encrypt)
      }
      if (config.threads > 1) {
        futures :+= future { f() }
      } else {
        f() match {
          case (hash, block) => writeProcessedBlock(hash, block)
        }
      }
      if (futures.size >= config.threads) {
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

  def readBlock(hash: Array[Byte], verifyHash: Boolean = false) = {
    setup()
    val hashS = encodeBase64Url(hash)
    l.trace(s"Getting block for hash $hashS")
    val num: Int = knownBlocks.get(hash).get
    val zipFile = getZipFileReader(num)
    //    if (!zipFile.file.exists()) {
    //      Actors.downloadFile(zipFile.file)
    //    }
    val input = zipFile.getStream(hashS)
    //val stream = StreamHeaders.readStream(input, config.passphrase)
    if (verifyHash) {
      new VerifyInputStream(input, config.getMessageDigest, hash, zipFile.file)
    } else {
      input
    }
  }

  def calculateOverhead(hashchains: Iterable[Array[Byte]]) = {
    var remain = Map[BAWrapper2, Int]()
    remain ++= knownBlocks
    hashchains.foreach(_.grouped(config.hashLength).foreach(x => remain -= BAWrapper2.byteArrayToWrapper(x)))
    val livingVolumes = remain.values.toSet
    println(livingVolumes.toList.sorted)
    val files = fileManager.index.getFiles()
    val deadNow = files.filter(x => !livingVolumes.contains(fileManager.index.getNum(x)))
    println(deadNow.mkString(" "))
    //    println(remain.toList.filter(_._2==258).mkString(" "))
    deadNow.map(_.length()).sum
  }

  var lastZip: Option[(Int, ZipFileReader)] = None

  override def finishReading() {
    lastZip.foreach { case (_, zip) => zip.close() }
    lastZip = None
  }

  def getZipFileReader(num: Int) = {
    lastZip match {
      case Some((n, zip)) if (n == num) => zip
      case _ => {
        lastZip.foreach { case (_, zip) => zip.close() }
        val out = new ZipFileReader(new File(config.folder, volumeName(num)))
        lastZip = Some((num, out))
        out
      }
    }
  }

}
