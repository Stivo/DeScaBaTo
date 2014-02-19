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
 * File patterns:
 * volume_num.zip => A zip file containing blocks with their hash used as filenames.
 * with the same number.
 * Only one instance can be used, it is currently not thread safe.
 */
class ZipBlockHandler extends StandardZipKeyValueStorage with BlockHandler with UniversePart {
  override def folder = "blocks/"
    
  lazy val indexFileType = fileManager.volumeIndex
  
  private val byteCounter = new MaxValueCounter() {
    var compressedBytes = 0
    def name: String = "Blocks written"
    def r(x: Long) = Utils.readableFileSize(x)
    override def formatted = s"${r(current)}/${r(maxValue)} (compressed ${r(compressedBytes)})"
  }

  private val compressionRatioCounter = new MaxValueCounter() {
    def name: String = "Compression Ratio"
    override def formatted = percent + "%"
  }

  def setTotalSize(size: Long) {
    byteCounter.maxValue = size
  }

  ProgressReporters.addCounter(byteCounter, compressionRatioCounter)

  protected def volumeSize = config.volumeSize

  def filetype = fileManager.volumes

  // Can not be called here, as the calling method is overwritten
  protected def shouldStartNextFile(w: ZipFileWriter, k: BAWrapper2, v: Array[Byte]): Boolean = ???

  protected var outstandingRequests: HashMap[BAWrapper2, Int] = HashMap()

  // TODO add in interface?
  def verify(problemCounter: ProblemCounter) = {
    // TODO implement
    // TODO check if key is in two volumes. Should be done in superclass
    true
  }

//  New concept: Check if block is twice in volumes. This should be implemented on the super class though
//  private def _init(verify: Boolean = false, problems: Option[Counter] = None) {
//    require(universe != null)
//    if (_initRan)
//      return
//    load()
//    // TODO
//    //      if (verify) {
//    //        val zip = getZipFileReader(num)
//    //        for (name <- zip.names.view.filter(_ != "manifest.txt")) {
//    //          if (!(lastSet safeContains (name))) {
//    //            problems.foreach(_ += 1)
//    //            l.warn("Index file is broken, does not contain " + name)
//    //          }
//    //          lastSet -= name
//    //        }
//    //        for (hash <- lastSet) {
//    //          problems.foreach(_ += 1)
//    //          l.warn("Hash " + hash + " is in index, but not in volume")
//    //        }
//    //        lastSet = Set.empty
//    //      }
//    //    }
//    _initRan = true
//  }

  private def asKey(hash: Array[Byte]): BAWrapper2 = hash

  def writeBlockIfNotExists(block: Block) {
    val hash = block.hash
    val k = asKey(hash)
    if (exists(k) || (outstandingRequests safeContains hash)) {
      byteCounter.maxValue -= block.content.length
      return
    }
    // TODO
    //    if (compressDisabled || config.compressor == CompressionMode.none) {
    //      val content = ByteBuffer.wrap(block)
    //      writeCompressedBlock(hash, ZipFileHandlerFactory.createZipEntry(.0, ByteBuffer.wrap(block))
    //    } else {
    block.mode = config.compressor
    outstandingRequests += ((hash, block.content.length))
//    universe.eventBus().publish(Add1CpuTask)
    universe.compressionDecider().compressBlock(block)
    //    }
  }

  def writeCompressedBlock(block: Block, zipEntry: ZipEntry) {
//    universe.eventBus().publish(Subtract1CpuTask)
    if (currentWriter != null && currentWriter.size + block.compressed.remaining() + zipEntry.getName().length > volumeSize.bytes) {
      endZipFile()
    }
    openZipFileWriter()
    byteCounter.compressedBytes += block.compressed.remaining()
    compressionRatioCounter += block.compressed.remaining()
    val hash: BAWrapper2 = block.hash
    currentWriter.writeUncompressedEntry(zipEntry, block.header, block.compressed)
    block.recycle()
    byteCounter += outstandingRequests(hash)
    compressionRatioCounter.maxValue += outstandingRequests(hash)
    outstandingRequests -= hash
    inCurrentWriterKeys += (hash)
  }

  def readBlock(hash: Array[Byte], verifyHash: Boolean): InputStream = {
    ensureLoaded()
    val file = inBackupIndex(hash)
    val stream = new ExceptionCatchingInputStream(CompressedStream.readStream(readValueAsStream(hash)),
      file)
    if (verifyHash) {
      new VerifyInputStream(stream, config.getMessageDigest, hash, file)
    } else {
      stream
    }
  }

  def getAllPersistedKeys(): Set[BAWrapper2] = {
    ensureLoaded()
    inBackupIndex.keySet
  }

  def isPersisted(hash: Array[Byte]) = {
    ensureLoaded()
    inBackupIndex safeContains hash
  }

  def remaining(): Int = {
    outstandingRequests.size
  }

  override def endZipFile() {
    if (currentWriter != null) {
      val file = currentWriter.file
      val num = filetype.numberOf(file)
      super.endZipFile()
      l.info("Finished volume " + num)
      universe.eventBus().publish(VolumeFinished(file, inBackupIndex.keySet))
    }
  }

  override def finish() = {
    if (remaining > 0) {
      throw new IllegalAccessException("Should not be called while still waiting for blocks")
    }
    super.finish
    true
  }

  def shutdown() = {
    super.finish
    ret
  }
  
}
