package ch.descabato.core.storage

import ch.descabato.utils.CompressedStream
import ch.descabato.utils.Implicits._
import ch.descabato.utils.Streams.ExceptionCatchingInputStream
import scala.collection.immutable.HashMap
import ch.descabato.core.BackupPartHandler
import ch.descabato.core.HashListHandler
import ch.descabato.core.BlockHandler
import java.io.File
import java.io.ByteArrayInputStream
import java.io.InputStream
import ch.descabato.core.BlockingOperation
import ch.descabato.core.Block
import java.util.zip.ZipEntry
import ch.descabato.core.UniversePart
import ch.descabato.core.VolumeFinished
import ch.descabato.core.ProblemCounter
import ch.descabato.core.BaWrapper

class KvStoreBackupPartHandler extends BackupPartHandler {
  def checkpoint(t: Option[(Set[ch.descabato.core.BaWrapper], Set[ch.descabato.core.BaWrapper])]): Unit = {
    ???
  }

  def fileFailed(fd: ch.descabato.core.FileDescription): Unit = {
    ???
  }

  def finish(): Boolean = {
    ???
  }

  def hashForFile(fd: ch.descabato.core.FileDescription, hash: Array[Byte]): Unit = {
    ???
  }

  def hashComputed(blockWrapper: ch.descabato.core.Block): Unit = {
    ???
  }

  def load(): Unit = {
    ???
  }

  def loadBackup(date: Option[java.util.Date]): ch.descabato.core.BackupDescription = {
    ???
  }

  def remaining(): Int = {
    ???
  }

  def setFiles(bd: ch.descabato.core.BackupDescription): Unit = {
    ???
  }

  def shutdown(): ch.descabato.core.BlockingOperation = {
    ???
  }
}

class KvStoreHashListHandler extends HashListHandler {
  
  def addHashlist(fileHash: Array[Byte], hashList: Array[Byte]): Unit = {
    ???
  }

  def checkpoint(t: Option[Set[ch.descabato.core.BaWrapper]]): Unit = {
    ???
  }

  def finish(): Boolean = {
    ???
  }

  def getAllPersistedKeys(): Set[ch.descabato.core.BaWrapper] = {
    ???
  }

  def getHashlist(fileHash: Array[Byte], size: Long): Seq[Array[Byte]] = {
    ???
  }

  def load(): Unit = {
    ???
  }

  def shutdown(): ch.descabato.core.BlockingOperation = {
    ???
  }
}

class KvStoreBlockHandler extends BlockHandler with UniversePart {
  lazy val fileType = fileManager.volumes

  var persistedEntries = Map.empty[BaWrapper, KvStoreLocation] 
  
  var currentlyWritingFile: KvStoreStorageMechanismWriter = null
  
  var _loaded = false
  
  private def getReaderForLocation(file: File) = {
    new KvStoreStorageMechanismReader(file, config.passphrase)
  }
  
  def finish(): Boolean = {
    if (currentlyWritingFile != null) {
      // update journal
      universe.journalHandler.finishedFile(currentlyWritingFile.file, fileType)
      universe.eventBus().publish(VolumeFinished(currentlyWritingFile.file, getAllPersistedKeys()));
      currentlyWritingFile.close()
      currentlyWritingFile = null
    }
    true
  }

  def getAllPersistedKeys(): Set[BaWrapper] = {
    ensureLoaded()
    persistedEntries.keySet
  }

  def isPersisted(hash: Array[Byte]): Boolean = getAllPersistedKeys() safeContains hash

  def load() {
    for (file <- fileType.getFiles(config.folder)) {
      val reader = getReaderForLocation(file)
      persistedEntries ++= reader.iterator.map {case (k, v) => (new BaWrapper(k), v)}
      reader.close()
    }
  }

  private def ensureLoaded() {
    if (!_loaded) {
      load()
      _loaded = true
    }
  }
  
  def readBlock(hash: Array[Byte], verify: Boolean): InputStream = {
    ensureLoaded()
    val pos = persistedEntries(new BaWrapper(hash))
    val reader = getReaderForLocation(pos.file)
    val out = new ByteArrayInputStream(reader.get(pos))
    reader.close()
    new ExceptionCatchingInputStream(CompressedStream.readStream(out), pos.file)
  }

  def remaining: Int = {
    outstandingRequests.size
  }

  def setTotalSize(size: Long): Unit = {
    // TODO
  }

  def shutdown(): BlockingOperation = {
    new BlockingOperation()
  }

  def verify(counter: ProblemCounter): Boolean = {
    // TODO
    true
  }

  protected var outstandingRequests: HashMap[BaWrapper, Int] = HashMap()

  def writeBlockIfNotExists(block: Block) {
    ensureLoaded()
    val hash = block.hash
    if (isPersisted(hash) || (outstandingRequests safeContains hash)) {
      //byteCounter.maxValue -= block.content.length
      return
    }
    block.mode = config.compressor
    outstandingRequests += ((hash, block.content.length))
    universe.compressionDecider().compressBlock(block)
   }

  def writeCompressedBlock(block: Block, zipEntry: ZipEntry) {
    if (currentlyWritingFile == null) {
      currentlyWritingFile =
        new KvStoreStorageMechanismWriter(fileType.nextFile(config.folder, false))
    }
    persistedEntries += new BaWrapper(block.hash) -> null
    currentlyWritingFile.add(block.hash, Array.apply(block.header) ++ block.compressed.toArray())
    outstandingRequests -= block.hash
    // TODO
//    if (currentWriter != null && currentWriter.size + block.compressed.remaining() + zipEntry.getName().length > volumeSize.bytes) {
//      endZipFile()
//    }
//    openZipFileWriter()
//    byteCounter.compressedBytes += block.compressed.remaining()
//    compressionRatioCounter += block.compressed.remaining()
//    byteCounter += outstandingRequests(hash)
//    compressionRatioCounter.maxValue += outstandingRequests(hash)
//    outstandingRequests -= hash
//    inCurrentWriterKeys += (hash)
  }
}