package ch.descabato.core

import java.io.File
import java.io.InputStream
import java.nio.file.Files
import net.java.truevfs.access.TFile
import net.java.truevfs.access.TFileInputStream
import java.nio.ByteBuffer
import ch.descabato.utils.Utils
import ch.descabato.utils.Streams._
import ch.descabato.frontend.Counter
import ch.descabato.utils.ZipFileWriter
import ch.descabato.utils.ZipFileHandlerFactory
import ch.descabato.utils.ZipFileReader
import ch.descabato.utils.CompressedStream
import ch.descabato.utils.Implicits._
import java.util.zip.ZipEntry
import scala.collection.mutable.HashMap
import ch.descabato.frontend.MaxValueCounter
import ch.descabato.frontend.ProgressReporters

/**
 * A block handler that creates zip files with parts of blocks in it.
 * Additionally for each volume an index is written, which stores the hashes
 * stored in that volume. So the volume itself is not needed for further backups,
 * only for restores.
 * File patterns:
 * volume_num.zip => A zip file containing blocks with their hash used as filenames.
 * index_num.zip => A zip file containing a file index with all hashes of the volume
 * with the same number.
 * Only one instance can be used, it is currently not thread safe.
 * To make it thread safe:
 * - Maps would have to be thread safe
 * - Different writers could use same file, but only with the TFile api
 */
class ZipBlockHandler extends BlockHandler with Utils with UniversePart {
  import Utils.encodeBase64Url
  private val byteCounter = new MaxValueCounter() {
    var compressedBytes = 0
    def name: String = "Blocks written"
    def r(x: Long) = Utils.readableFileSize(x)
    override def formatted = s"${r(current)}/${r(maxValue)} (compressed ${r(compressedBytes)})"
  }

  private val compressionRatioCounter = new MaxValueCounter() {
    def name: String = "Compression Ratio"
    override def formatted = percent+"%"
  }

  def setTotalSize(size: Long) {
    byteCounter.maxValue = size
  }
  
  def updateProgress() {
    ProgressReporters.updateWithCounters(byteCounter::compressionRatioCounter::Nil)
  }
  
  protected def volumeSize = config.volumeSize
  // All persisted blocks
  protected var knownBlocks: Map[BAWrapper2, Int] = Map()
  // The blocks that have been added but not yet persisted
  protected var knownBlocksTemp: Map[BAWrapper2, Int] = Map()
  // The blocks that have been written with the last volume.
  // These blocks are not yet persisted, but will be when the zip
  // file is ending
  protected var knownBlocksWritten: Map[BAWrapper2, Int] = Map()
  protected var _initRan = false
  protected var curNum = 0
  protected var currentZip: ZipFileWriter = null
  protected var currentIndex: IndexWriter = null
  protected var lastZip: Option[(Int, ZipFileReader)] = None
  @volatile
  protected var outstandingRequests: HashMap[BAWrapper2, Int] = HashMap()

  // TODO add in interface?
  def verify(problemCounter: Counter) {
    _init(true, Some(problemCounter))
  }

  override def setupInternal() {
    _init(false)
  }
  
  private def _init(verify: Boolean = false, problems: Option[Counter] = None) {
    require(universe != null)
    if (_initRan)
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
        for (name <- zip.names.view.filter(_ != "manifest.txt")) {
          if (!(lastSet safeContains (name))) {
            problems.foreach(_ += 1)
            l.warn("Index file is broken, does not contain " + name)
          }
          lastSet -= name
        }
        for (hash <- lastSet) {
          problems.foreach(_ += 1)
          l.warn("Hash " + hash + " is in index, but not in volume")
        }
        lastSet = Set.empty
      }
    }
    curNum = index.nextNum()
    _initRan = true
  }

  def deleteTempFiles() {
    def deleteFile(x: File) = {
      l.debug("Deleting temporary file " + x)
      if (!x.delete) {
        l.warn("Could not delete temporary file " + x)
      }
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
    fileManager.index.getFiles().filter(x => set safeContains (fileManager.index.getNum(x))).foreach(deleteFile)
    fileManager.volumes.getFiles().filter(x => set safeContains (fileManager.volumes.getNum(x))).foreach(deleteFile)
  }

  private def asKey(hash: Array[Byte]): BAWrapper2 = hash

  def writeBlockIfNotExists(blockId: BlockId, hash: Array[Byte], block: Array[Byte], compressDisabled: Boolean) {
    val k = asKey(hash)
    if ((knownBlocks safeContains k) || (knownBlocksTemp safeContains k)) {
      byteCounter.maxValue -= block.length
      updateProgress
      return
    }
    knownBlocksTemp += ((hash, curNum))
    // TODO
    //    if (compressDisabled || config.compressor == CompressionMode.none) {
    //      val content = ByteBuffer.wrap(block)
    //      writeCompressedBlock(hash, ZipFileHandlerFactory.createZipEntry(.0, ByteBuffer.wrap(block))
    //    } else {
    outstandingRequests += ((hash, block.length))
    universe.cpuTaskHandler.compress(blockId, hash, block, config.compressor, compressDisabled)
    //    }
  }

  def writeCompressedBlock(hash: Array[Byte], zipEntry: ZipEntry, header: Byte, block: ByteBuffer) {
    if (currentZip != null && currentZip.size + block.remaining() + zipEntry.getName().length > volumeSize.bytes) {
      endZip
    }
    if (currentZip == null) {
      startZip
    }
    byteCounter.compressedBytes += block.remaining()
    compressionRatioCounter += block.remaining()
    currentZip.writeUncompressedEntry(zipEntry, header, block)
    block.recycle()
    currentIndex.bos.write(hash)
    byteCounter += outstandingRequests(hash)
    compressionRatioCounter.maxValue += outstandingRequests(hash)
    updateProgress
    outstandingRequests -= hash
    knownBlocksWritten += ((hash, curNum))
  }

  // or multiple blocks
  def blockIsPersisted(hash: Array[Byte]): Boolean = {
    _init()
    knownBlocks safeContains hash
  }

  def readBlock(hash: Array[Byte], verifyHash: Boolean): InputStream = {
    _init()
    val hashS = encodeBase64Url(hash)
    l.trace(s"Getting block for hash $hashS")
    val num: Int = knownBlocks.get(hash).get
    val zipFile = getZipFileReader(num)

    val input = zipFile.getStream(hashS)
    val stream = new ExceptionCatchingInputStream(CompressedStream.readStream(input), zipFile.file)
    if (verifyHash) {
      new VerifyInputStream(stream, config.getMessageDigest, hash, zipFile.file)
    } else {
      stream
    }
  }

  def remaining(): Int = {
    outstandingRequests.size
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
    lazy val zipWriter = ZipFileHandlerFactory.writer(x, config)
    lazy val bos = zipWriter.newOutputStream("index")
    def close() {
      bos.close()
      zipWriter.close
    }
  }

  def startZip() {
    l.info(s"Starting volume ${volumeName(curNum)}")
    currentZip = ZipFileHandlerFactory.writer(new File(config.folder, volumeName(curNum, true)), config)
    currentIndex = new IndexWriter(new File(config.folder, indexName(curNum, true)))
  }

  def endZip() {
    if (currentZip != null) {
      l.info(s"Ending zip file $curNum")
      currentZip.writeManifest(fileManager)
      currentZip.close()
      currentIndex.bos.close()
      currentIndex.zipWriter.writeManifest(fileManager)
      currentIndex.close()
      // TODO journal makes this obsolete
      def rename(f: (Int, Boolean) => String) = {
        val from = new File(config.folder, f(curNum, true))
        val to = new File(config.folder, f(curNum, false))
        Files.move(from.toPath(), to.toPath())
        val success = to.exists()
        if (!success)
          l.warn("Could not rename file from " + from + " to " + to)
        success
      }
      val bothRenamed = rename(volumeName) && rename(indexName)
      if (bothRenamed) {
        universe.journalHandler.finishedFile(volumeName(curNum, false))
        universe.journalHandler.finishedFile(indexName(curNum, false))
      }
      knownBlocks ++= knownBlocksWritten
      knownBlocksTemp --= knownBlocksWritten.keys
      knownBlocksWritten = Map()
      curNum += 1
      currentZip = null
      currentIndex = null
    }
  }

  def finish() = {
    if (remaining > 0)
      false
    else {
      endZip
      finishReading
      true
    }
  }

  def finishReading() {
    lastZip.foreach { case (_, zip) => zip.close() }
    lastZip = None
  }

  private def getZipFileReader(num: Int) = {
    lastZip match {
      case Some((n, zip)) if n == num => zip
      case _ =>
        lastZip.foreach { case (_, zip) => zip.close() }
        val out = ZipFileHandlerFactory.reader(new File(config.folder, volumeName(num)), config)
        lastZip = Some((num, out))
        out
    }
  }

}
