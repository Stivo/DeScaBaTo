package ch.descabato.core.storage

import java.io.{ByteArrayInputStream, File, InputStream}
import java.util.zip.ZipEntry

import ch.descabato.core._
import ch.descabato.utils.CompressedStream
import ch.descabato.utils.Implicits._
import ch.descabato.utils.Streams.{VerifyInputStream, ExceptionCatchingInputStream}

import scala.collection.immutable.HashMap

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
  lazy val fileType = fileManager.hashlists

  var persistedEntries = Map.empty[BaWrapper, KvStoreLocation]

  var currentlyWritingFile: KvStoreStorageMechanismWriter = null

  var _loaded = false

  private def getReaderForLocation(file: File) = {
    new KvStoreStorageMechanismReader(file, config.passphrase)
  }

  def addHashlist(fileHash: Array[Byte], hashList: Array[Byte]): Unit = {
    ensureLoaded()
    if (persistedEntries safeContains fileHash)
      return
    if (currentlyWritingFile == null) {
      currentlyWritingFile =
        new KvStoreStorageMechanismWriter(fileType.nextFile(config.folder, false), config.passphrase)
    }
    persistedEntries += new BaWrapper(fileHash) -> null
    currentlyWritingFile.add(fileHash, hashList)
  }

  def checkpoint(t: Option[Set[ch.descabato.core.BaWrapper]]): Unit = {
    if (currentlyWritingFile != null) {
      currentlyWritingFile.checkpoint()
    }
  }

  def finish(): Boolean = {
    if (currentlyWritingFile != null) {
      currentlyWritingFile.close()
      universe.eventBus().publish(HashListCheckpointed(getAllPersistedKeys, universe.blockHandler().getAllPersistedKeys()))
      currentlyWritingFile = null
    }
    true
  }

  def getAllPersistedKeys(): Set[ch.descabato.core.BaWrapper] = {
    ensureLoaded()
    persistedEntries.keySet
  }

  def getHashlist(fileHash: Array[Byte], size: Long): Seq[Array[Byte]] = {
    ensureLoaded()
    val pos = persistedEntries(fileHash)
    val reader = getReaderForLocation(pos.file)
    val out = reader.get(pos)
    reader.close()
    out.grouped(config.hashLength).toSeq
  }

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

  def shutdown(): ch.descabato.core.BlockingOperation = {
    finish()
    new BlockingOperation()
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
    val out2 = new ExceptionCatchingInputStream(CompressedStream.readStream(out), pos.file)
    if (verify) {
      new VerifyInputStream(out2, config.getMessageDigest(), hash, pos.file)
    } else {
      out2
    }
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
        new KvStoreStorageMechanismWriter(fileType.nextFile(config.folder, false), config.passphrase)
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