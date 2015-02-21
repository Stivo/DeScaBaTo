package ch.descabato.core.storage

import java.io.{File, InputStream}
import java.util

import ch.descabato.CompressionMode
import ch.descabato.core._
import ch.descabato.frontend.{MaxValueCounter, ProgressReporters}
import ch.descabato.utils.Implicits._
import ch.descabato.utils.Streams.{ExceptionCatchingInputStream, VerifyInputStream}
import ch.descabato.utils._

import scala.collection.immutable.HashMap
import scala.concurrent.Future
import scala.language.reflectiveCalls

trait KvStoreHandler[KI, KM, T] extends UniversePart {
  def fileType: FileType[T]

  var persistedEntries = Map.empty[KM, KvStoreLocation]

  var currentlyWritingFile: KvStoreStorageMechanismWriter = null

  var _loaded = false

  var readers: HashMap[File, KvStoreStorageMechanismReader] = HashMap.empty

  protected def getReaderForLocation(file: File) = {
    if (!readers.safeContains(file)) {
      readers += file -> new KvStoreStorageMechanismReader(file, config.passphrase)
    }
    readers(file)
  }

  def load() {
    for (file <- fileType.getFiles(config.folder)) {
      val reader = getReaderForLocation(file)
      persistedEntries ++= reader.iterator.map {
        case (k, v) => (storageToKeyMem(k), v)
      }
    }
  }

  protected def ensureLoaded() {
    if (!_loaded) {
      load()
      _loaded = true
    }
  }

  def shutdown(): ch.descabato.core.BlockingOperation = {
    finish()
    new BlockingOperation()
  }

  def finish(): Boolean = {
    readers.values.foreach(_.close)
    readers = HashMap.empty
    if (currentlyWritingFile != null) {
      currentlyWritingFile.close()
      fileFinished()
      currentlyWritingFile = null
    }
    true
  }

  var entries = 0

  def checkIfCheckpoint(): Boolean = {
    entries += 1
    entries % 10 == 0
  }

  def writeEntry(key: KI, value: Array[Byte]): Unit = {
    if (currentlyWritingFile == null) {
      currentlyWritingFile =
        new KvStoreStorageMechanismWriter(fileType.nextFile(config.folder, temp = false), config.passphrase)
      currentlyWritingFile.setup(universe)
      currentlyWritingFile.writeManifest()
    }
    val memKey = keyInterfaceToKeyMem(key)
    persistedEntries += memKey -> null
    currentlyWritingFile.add(keyMemToStorage(memKey), value)
    if (checkIfCheckpoint())
      currentlyWritingFile.checkpoint()
  }

  def keyMemToStorage(k: KM): Array[Byte]

  def storageToKeyMem(k: Array[Byte]): KM

  def keyInterfaceToKeyMem(k: KI): KM

  def keyMemToKeyInterface(k: KM): KI

  def readEntry(key: KI) = {
    ensureLoaded()
    val pos = persistedEntries(keyInterfaceToKeyMem(key))
    val reader = getReaderForLocation(pos.file)
    val out = reader.get(pos)
    (out, pos)
  }

  def fileFinished()
}

trait SimpleKvStoreHandler[K, V] extends KvStoreHandler[K, K, V] {
  def keyInterfaceToKeyMem(k: K): K = k

  def keyMemToKeyInterface(k: K) = k
}

trait HashKvStoreHandler[V] extends KvStoreHandler[Hash, BaWrapper, V] {
  def keyInterfaceToKeyMem(k: Hash): BaWrapper = new BaWrapper(k.bytes)
  def keyMemToKeyInterface(k: BaWrapper) = new Hash(k.data)
  def keyMemToStorage(k: BaWrapper): Array[Byte] = k.data
  def storageToKeyMem(k: Array[Byte]) = new BaWrapper(k)
  def isPersisted(fileHash: Hash): Boolean = {
    ensureLoaded()
    val mapKey = keyInterfaceToKeyMem(fileHash)
    persistedEntries safeContains(mapKey)
  }
}

class KvStoreBackupPartHandler extends SimpleKvStoreHandler[String, BackupDescription] with BackupPartHandler with Utils with BackupProgressReporting {

  override val filecountername = "Files hashed"
  override val bytecountername = "Data hashed"
  override def nameOfOperation: String = "Should not be shown, is a bug"

  var current: BackupDescription = new BackupDescription()
  var failedObjects = new BackupDescription()

  protected var unfinished: Map[String, FileDescriptionWrapper] = Map.empty

  def blocksFor(fd: FileDescription) = {
    if (fd.size == 0) 1
    else (1.0 * fd.size / config.blockSize.bytes).ceil.toInt
  }

  class FileDescriptionWrapper(var fd: FileDescription) {
    lazy val blocks: util.BitSet = {
      val out = new util.BitSet(blocksFor(fd))
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

    def fileHashArrived(hash: Hash) {
      fd = fd.copy(hash = hash)
      checkFinished()
    }

    private def checkFinished() {
      if (blocks.isEmpty && fd.hash.isNotNull && !failed) {
        writeBackupPart(fd)
        if (hashList.length > fd.hash.length)
          universe.hashListHandler.addHashlist(fd.hash, hashList)
        fileCounter += 1
        unfinished -= fd.path
      }
    }

    def blockHashArrived(blockId: BlockId, hash: Hash) {
      if (!failed) {
        blocks.clear(blockId.part)
        System.arraycopy(hash.bytes, 0, hashList, blockId.part * config.hashLength, config.hashLength)
        checkFinished()
      }
    }
  }

  def fileFailed(fd: FileDescription): Unit = {
    getUnfinished(fd).setFailed()
  }

  def hashForFile(fd: FileDescription, hash: Hash): Unit = {
    getUnfinished(fd).fileHashArrived(hash)
  }

  def hashComputed(block: Block): Unit = {
    universe.blockHandler.writeBlockIfNotExists(block)
    byteCounter += block.content.length
    getUnfinished(block.id.file).blockHashArrived(block.id, block.hash)
  }

  def loadBackup(date: Option[java.util.Date]): ch.descabato.core.BackupDescription = {
    val filesToLoad: Seq[File] = date match {
      case Some(d) => fileManager.getBackupForDate(d)
      case None => fileManager.getLastBackup(temp = true)
    }
    current = new BackupDescription()
    val kvstores = filesToLoad.view.map(getReaderForLocation)
    kvstores.foreach {
      kvstore =>
        kvstore.iterator.foreach {
          case (entry, pos) =>
            persistedEntries += storageToKeyMem(entry) -> pos
            val value = kvstore.get(pos)
            val decompressed = CompressedStream.decompress(value).readFully()
            js.read[BackupPart](decompressed) match {
              case Left(x: UpdatePart) => current += x
              case x => throw new IllegalArgumentException("Not implemented for object "+x)
            }
        }
    }
    current
  }

  def remaining(): Int = {
    unfinished.count(!_._2.failed)
  }

  def setFiles(finished: BackupDescription, unfinished: BackupDescription) {
    current = finished.merge(unfinished)
    unfinished.files.foreach {
      file =>
        getUnfinished(file.copy(hash = NullHash.nul))
    }
    val toCheckpoint = finished
    toCheckpoint.allParts.foreach(writeBackupPart)
    unfinished.folders.foreach(writeBackupPart)
    setMaximums(current)
    l.info("After setCurrent " + remaining + " remaining")
  }

  val js = new JsonSerialization()

  def writeBackupPart(fd: BackupPart) {
    val json = js.write(fd)
    val compressed = CompressedStream.compress(json, CompressionMode.deflate)
    writeEntry(fd.path, compressed.toArray())
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

  override def keyMemToStorage(k: String) = k.getBytes("UTF-8")
  override def storageToKeyMem(k: Array[Byte]) = new String(k, "UTF-8")

}

class KvStoreHashListHandler extends HashKvStoreHandler[Vector[(Hash, Array[Byte])]] with HashListHandler {
  lazy val fileType = fileManager.hashlists

  def addHashlist(fileHash: Hash, hashList: Array[Byte]): Unit = {
    ensureLoaded()
    if (isPersisted(fileHash))
      return
    writeEntry(fileHash, hashList)
  }

  def getHashlist(fileHash: Hash, size: Long): Seq[Hash] = {
    readEntry(fileHash)._1.grouped(config.hashLength).toSeq.map(new Hash(_))
  }

  def fileFinished() {
    universe.journalHandler().finishedFile(currentlyWritingFile.file, fileType)
  }
}

class KvStoreBlockHandler extends HashKvStoreHandler[Volume] with BlockHandler with Utils {
  lazy val fileType = fileManager.volumes

  private val byteCounter = new MaxValueCounter() {
    var compressedBytes = 0L
    def name: String = "Blocks written"
    def r(x: Long) = Utils.readableFileSize(x)
    override def formatted = s"${r(current)}/${r(maxValue)} (compressed ${r(compressedBytes)})"
  }

  private val compressionRatioCounter = new MaxValueCounter() {
    def name: String = "Compression Ratio"
    override def formatted = percent + "%"
  }

  ProgressReporters.addCounter(byteCounter, compressionRatioCounter)

  override def keyMemToStorage(k: BaWrapper): Array[Byte] = k.data
  override def storageToKeyMem(k: Array[Byte]) = new BaWrapper(k)

  def fileFinished() {
    universe.journalHandler.finishedFile(currentlyWritingFile.file, fileType)
  }

  def readBlock(hash: Hash, verify: Boolean): InputStream = {
    val (entry, pos) = readEntry(hash)
    val out2 = new ExceptionCatchingInputStream(CompressedStream.decompress(entry), pos.file)
    if (verify) {
      new VerifyInputStream(out2, config.getMessageDigest(), hash.bytes, pos.file)
    } else {
      out2
    }
  }

  override def readBlockAsync(hash: Hash): Future[Array[Byte]] = {
    Future.successful {
      readBlock(hash)
    }
  }

  override def readBlock(hash: Hash): Array[Byte] = {
      readEntry(hash)._1
  }

  override def checkIfCheckpoint() = false

  def remaining: Int = {
    outstandingRequests.size
  }

  def setTotalSize(size: Long): Unit = {
    byteCounter.maxValue = size
  }

  def verify(counter: ProblemCounter): Boolean = {
    // TODO
    true
  }

  protected var outstandingRequests: HashMap[BaWrapper, Int] = HashMap()

  def writeBlockIfNotExists(block: Block) {
    val hash = block.hash
    if (isPersisted(hash) || (outstandingRequests safeContains hash)) {
      byteCounter.maxValue -= block.content.length
      return
    }
    block.mode = config.compressor
    outstandingRequests += ((hash, block.content.length))
    universe.compressionDecider().compressBlock(block)
   }

  def writeCompressedBlock(block: Block) {

    if (currentlyWritingFile != null) {
      val currentSize = currentlyWritingFile.length() + block.compressed.remaining() + config.hashLength + 50
      if (currentSize > config.volumeSize.bytes) {
        currentlyWritingFile.close()
        fileFinished()
        currentlyWritingFile = null
      }
    }
    writeEntry(block.hash, block.compressed.toArray())
    byteCounter += outstandingRequests(block.hash)
    compressionRatioCounter.maxValue += outstandingRequests(block.hash)
    outstandingRequests -= block.hash
    byteCounter.compressedBytes += block.compressed.remaining()
    compressionRatioCounter += block.compressed.remaining()
  }

}