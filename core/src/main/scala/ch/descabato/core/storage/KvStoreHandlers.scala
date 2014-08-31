package ch.descabato.core.storage

import java.io.{ByteArrayInputStream, File, InputStream}
import java.util.BitSet
import java.util.zip.ZipEntry

import ch.descabato.core._
import ch.descabato.utils.{JsonSerialization, ObjectPools, CompressedStream, Utils}
import ch.descabato.utils.Implicits._
import ch.descabato.utils.Streams.{VerifyInputStream, ExceptionCatchingInputStream}

import scala.collection.immutable.HashMap

trait KvStoreHandler[T, K] extends UniversePart {
  def fileType: FileType[T]

  var persistedEntries = Map.empty[K, KvStoreLocation]

  var currentlyWritingFile: KvStoreStorageMechanismWriter = null

  var _loaded = false

  protected def getReaderForLocation(file: File) = {
    new KvStoreStorageMechanismReader(file, config.passphrase)
  }

  def load() {
    for (file <- fileType.getFiles(config.folder)) {
      val reader = getReaderForLocation(file)
      reader.kvstoreReader.checkAndFixFile()
      persistedEntries ++= reader.iterator.map {
        case (k, v) => (storageToKey(k), v)
      }
      reader.close()
    }
  }

  protected def ensureLoaded() {
    if (!_loaded) {
      load()
      _loaded = true
    }
  }

//  def checkpoint(t: Option[Set[ch.descabato.core.BaWrapper]]): Unit = {
//    if (currentlyWritingFile != null) {
//      currentlyWritingFile.checkpoint()
//    }
//  }

  def getAllPersistedKeys(): Set[K] = {
    ensureLoaded()
    persistedEntries.keySet
  }

  def shutdown(): ch.descabato.core.BlockingOperation = {
    finish()
    new BlockingOperation()
  }

  def finish(): Boolean = {
    if (currentlyWritingFile != null) {
      currentlyWritingFile.close()
      fileFinished()
      currentlyWritingFile = null
    }
    true
  }

  def writeEntry(key: K, value: Array[Byte]): Unit = {
    if (currentlyWritingFile == null) {
      currentlyWritingFile =
        new KvStoreStorageMechanismWriter(fileType.nextFile(config.folder, false), config.passphrase)
    }
    persistedEntries += key -> null
    currentlyWritingFile.add(keyToStorage(key), value)
  }

  def keyToStorage(k: K): Array[Byte]

  def storageToKey(k: Array[Byte]): K

  def readEntry(key: K) = {
    ensureLoaded()
    val pos = persistedEntries(key)
    val reader = getReaderForLocation(pos.file)
    val out = reader.get(pos)
    reader.close()
    (out, pos)
  }

  def fileFinished()
}


class KvStoreBackupPartHandler extends KvStoreHandler[BackupDescription, String] with BackupPartHandler with Utils {

  var current: BackupDescription = new BackupDescription()
  var failedObjects = new BackupDescription()

  protected var unfinished: Map[String, FileDescriptionWrapper] = Map.empty

  def blocksFor(fd: FileDescription) = {
    if (fd.size == 0) 1
    else (1.0 * fd.size / config.blockSize.bytes).ceil.toInt
  }

  class FileDescriptionWrapper(var fd: FileDescription) {
    lazy val blocks: BitSet = {
      val out = new BitSet(blocksFor(fd))
      out.set(0, blocksFor(fd), true)
      out
    }
    var failed = false
    var hashList = ObjectPools.byteArrayPool.getExactly(blocksFor(fd) * config.hashLength)

    def setFailed() {
      failed = true
      failedObjects += fd
      ObjectPools.byteArrayPool.recycle(hashList)
      hashList = null
      unfinished -= fd.path
    }

    def fileHashArrived(hash: Array[Byte]) {
      fd = fd.copy(hash = hash)
      checkFinished()
    }

    private def checkFinished() {
      if (blocks.isEmpty && fd.hash != null && !failed) {
        writeBackupPart(fd)
        if (hashList.length > fd.hash.length)
          universe.hashListHandler.addHashlist(fd.hash, hashList)
        // TODO fileCounter += 1
        unfinished -= fd.path
      }
    }

    def blockHashArrived(blockId: BlockId, hash: Array[Byte]) {
      if (!failed) {
        blocks.clear(blockId.part)
        System.arraycopy(hash, 0, hashList, blockId.part * config.hashLength, config.hashLength)
        checkFinished()
      }
    }
  }


  def checkpoint(t: Option[(Set[BaWrapper], Set[BaWrapper])]): Unit = {

  }

  def fileFailed(fd: FileDescription): Unit = {
    getUnfinished(fd).setFailed()
  }

  def hashForFile(fd: FileDescription, hash: Array[Byte]): Unit = {
    getUnfinished(fd).fileHashArrived(hash)
  }

  def hashComputed(block: Block): Unit = {
    universe.blockHandler.writeBlockIfNotExists(block)
    // TODO byteCounter += block.content.length
    getUnfinished(block.id.file).blockHashArrived(block.id, block.hash)
  }

  def loadBackup(date: Option[java.util.Date]): ch.descabato.core.BackupDescription = {
    val filesToLoad: Seq[File] = fileManager.getLastBackup(temp = true)
    current = new BackupDescription()
    val kvstores = filesToLoad.view.map(getReaderForLocation)
    kvstores.foreach {
      kvstore =>
        kvstore.iterator.foreach {
          case (entry, pos) =>
            persistedEntries += storageToKey(entry) -> pos
            val value = kvstore.get(pos)
            js.read[BackupPart](value) match {
              case Left(x: UpdatePart) => current += x
              case x => throw new IllegalArgumentException("Not implemented for object "+x)
            }
        }
    }
    current
  }

  def remaining(): Int = {
    unfinished.filter(!_._2.failed).size
  }

  def setFiles(bd: BackupDescription) {
    current = bd
    bd.files.foreach {
      file =>
        if (file.hash == null)
          getUnfinished(file)
    }
    val toCheckpoint = current.copy(files = current.files.filter(_.hash != null))
    toCheckpoint.allParts.foreach(writeBackupPart)
    // TODO setMaximums(bd)
    //toCheckpoint = current.copy(files = current.files.filter(_.hash != null))
    l.info("After setCurrent " + remaining + " remaining")
  }

  val js = new JsonSerialization()

  def writeBackupPart(fd: BackupPart) {
    val json = js.write(fd)
    writeEntry(fd.path, json)
  }

  protected def getUnfinished(fd: FileDescription) =
    unfinished.get(fd.path) match {
      case Some(w) => w
      case None =>
        val out = new FileDescriptionWrapper(fd)
        unfinished += fd.path -> out
        out
  }

  lazy val fileType = universe.fileManager().backup

  override def fileFinished() = {
    universe.journalHandler().finishedFile(currentlyWritingFile.file, fileType)    // TODO
  }

  override def keyToStorage(k: String) = k.getBytes("UTF-8")
  override def storageToKey(k: Array[Byte]) = new String(k, "UTF-8")
}

class KvStoreHashListHandler extends KvStoreHandler[Vector[(BaWrapper, Array[Byte])], BaWrapper] with HashListHandler {
  lazy val fileType = fileManager.hashlists

  def addHashlist(fileHash: Array[Byte], hashList: Array[Byte]): Unit = {
    ensureLoaded()
    if (persistedEntries safeContains fileHash)
      return
    writeEntry(fileHash, hashList)
  }

  def getHashlist(fileHash: Array[Byte], size: Long): Seq[Array[Byte]] = {
    readEntry(fileHash)._1.grouped(config.hashLength).toSeq
  }

  def fileFinished() {
    universe.journalHandler().finishedFile(currentlyWritingFile.file, fileType)
  }

  override def keyToStorage(k: BaWrapper): Array[Byte] = k.data
  override def storageToKey(k: Array[Byte]) = new BaWrapper(k)

  override def checkpoint(t: Option[Set[BaWrapper]]): Unit = {}
}

class KvStoreBlockHandler  extends KvStoreHandler[Volume, BaWrapper] with BlockHandler {
  lazy val fileType = fileManager.volumes

  override def keyToStorage(k: BaWrapper): Array[Byte] = k.data
  override def storageToKey(k: Array[Byte]) = new BaWrapper(k)

  def fileFinished() {
    universe.journalHandler.finishedFile(currentlyWritingFile.file, fileType)
  }

  def isPersisted(hash: Array[Byte]): Boolean = getAllPersistedKeys() safeContains hash

  def readBlock(hash: Array[Byte], verify: Boolean): InputStream = {
    val (entry, pos) = readEntry(hash)
    val out2 = new ExceptionCatchingInputStream(CompressedStream.readStream(new ByteArrayInputStream(entry)), pos.file)
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

  def verify(counter: ProblemCounter): Boolean = {
    // TODO
    true
  }

  protected var outstandingRequests: HashMap[BaWrapper, Int] = HashMap()

  def writeBlockIfNotExists(block: Block) {
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
    writeEntry(block.hash, Array.apply(block.header) ++ block.compressed.toArray())
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