package ch.descabato.core.storage

import java.io.File

import ch.descabato.{CompressionMode, CustomByteArrayOutputStream}
import ch.descabato.core._
import ch.descabato.frontend.{MaxValueCounter, ProgressReporters}
import ch.descabato.utils.Implicits._
import ch.descabato.utils._

import scala.collection.immutable.{HashMap, TreeMap}
import scala.collection.mutable
import scala.concurrent.Future
import scala.language.reflectiveCalls

trait KvStoreHandler[KI, KM, T] extends UniversePart {
  def fileType: FileType[T]

  var persistedEntries: mutable.Map[KM, KvStoreLocation] = initMap()

  def initMap(): mutable.Map[KM, KvStoreLocation]

  var currentlyWritingFile: KvStoreStorageMechanismWriter = _

  var _loaded = false

  var readers: HashMap[File, KvStoreStorageMechanismReader] = HashMap.empty

  protected def getReaderForLocation(file: File): KvStoreStorageMechanismReader = {
    if (!readers.safeContains(file)) {
      readers += file -> new KvStoreStorageMechanismReader(file, config.passphrase)
    }
    readers(file)
  }

  final def load() {
    for (file <- fileType.getFiles(config.folder)) {
      loadFile(file)
    }
  }

  def loadFile(file: File): Unit = {
    val reader = getReaderForLocation(file)
    persistedEntries ++= reader.iterator.map {
      case (k, v) => (storageToKeyMem(k), v)
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

  def writeEntry(key: KI, value: BytesWrapper): Unit = {
    if (currentlyWritingFile == null) {
      currentlyWritingFile =
        new KvStoreStorageMechanismWriter(fileType.nextFile(config.folder), config.passphrase)
      currentlyWritingFile.setup(universe)
      currentlyWritingFile.writeManifest()
    }
    val memKey = keyInterfaceToKeyMem(key)
    val pos = currentlyWritingFile.add(keyMemToStorage(memKey), value)
    persistedEntries += memKey -> pos
    if (checkIfCheckpoint())
      currentlyWritingFile.checkpoint()
  }

  def keyMemToStorage(k: KM): Array[Byte]

  def storageToKeyMem(k: Array[Byte]): KM

  def keyInterfaceToKeyMem(k: KI): KM

  def keyMemToKeyInterface(k: KM): KI

  def readEntry(key: KI): (BytesWrapper, KvStoreLocation) = {
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

  def keyMemToKeyInterface(k: K): K = k
}

trait HashKvStoreHandler[V] extends KvStoreHandler[Hash, Array[Byte], V] {

  def initMap = new ByteArrayKeyedMap[KvStoreLocation]()

  def keyInterfaceToKeyMem(k: Hash): Array[Byte] = k.bytes

  def keyMemToKeyInterface(k: Array[Byte]) = new Hash(k)

  def keyMemToStorage(k: Array[Byte]): Array[Byte] = k

  def storageToKeyMem(k: Array[Byte]): Array[Byte] = k

  def isPersisted(fileHash: Hash): Boolean = {
    ensureLoaded()
    persistedEntries.contains(keyInterfaceToKeyMem(fileHash))
  }
}

class KvStoreBackupPartHandler extends SimpleKvStoreHandler[String, BackupDescription] with BackupPartHandler with Utils with BackupProgressReporting {

  override val filecountername = "Files hashed"
  override val bytecountername = "Data hashed"

  override def nameOfOperation: String = "Should not be shown, is a bug"

  var loadedBackup: Seq[File] = Nil

  var current: BackupDescription = BackupDescription()
  var failedObjects = BackupDescription()

  def initMap = new mutable.HashMap[String, KvStoreLocation]()

  protected var unfinished: Map[String, FileDescriptionWrapper] = Map.empty

  class FileDescriptionWrapper(var fd: FileDescription) {

    private var totalBytes = 0

    var failed = false
    private var hashLists: TreeMap[Int, Array[Byte]] = TreeMap.empty

    def setFailed() {
      failed = true
      failedObjects += fd
      hashLists = null
      unfinished -= fd.path
    }

    def fileHashArrived(hash: Hash) {
      fd = fd.copy(hash = hash)
      checkFinished()
    }

    private def checkFinished() {
      if (totalBytes == fd.size && fd.hash.isNotNull && !failed) {
        fd.hasHashList = hashLists.size > 1
        writeBackupPart(fd)
        if (hashLists.size > 1) {
          val stream = new CustomByteArrayOutputStream(config.hashLength * hashLists.size)
          for (elem <- hashLists.values) {
            stream.write(elem)
          }
          universe.hashListHandler.addHashlist(fd.hash, stream.toBytesWrapper.asArray())
        }
        fileCounter += 1
        unfinished -= fd.path
      }
    }

    def blockHashArrived(block: Block) {
      if (!failed) {
        totalBytes += block.content.length
        hashLists += block.id.part -> block.hash
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
    getUnfinished(block.id.file).blockHashArrived(block)
  }

  def loadBackup(date: Option[java.util.Date]): ch.descabato.core.BackupDescription = {
    loadedBackup = date match {
      case Some(d) => fileManager.getBackupForDate(d)
      case None => fileManager.getLastBackup(temp = true)
    }
    current = BackupDescription()
    val kvstores = loadedBackup.view.map(getReaderForLocation)
    kvstores.foreach {
      kvstore =>
        kvstore.iterator.foreach {
          case (entry, pos) =>
            persistedEntries += storageToKeyMem(entry) -> pos
            val value = kvstore.get(pos)
            val decompressed = CompressedStream.decompressToBytes(value)
            js.read[BackupPart](decompressed) match {
              case Left(x: UpdatePart) => current += x
              case x => throw new IllegalArgumentException("Not implemented for object " + x)
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
    val compressed = CompressedStream.compress(json.wrap(), CompressionMode.deflate)
    writeEntry(fd.path, compressed)
  }

  protected def getUnfinished(fd: FileDescription): FileDescriptionWrapper =
    unfinished.get(fd.path) match {
      case Some(w) => w
      case None =>
        val out = new FileDescriptionWrapper(fd)
        unfinished += fd.path -> out
        out
    }

  lazy val fileType: FileType[BackupDescription] = universe.fileManager().backup

  override def fileFinished(): Unit = {
    universe.journalHandler().finishedFile(currentlyWritingFile.file, fileType) // TODO
  }

  override def keyMemToStorage(k: String): Array[Byte] = k.getBytes("UTF-8")

  override def storageToKeyMem(k: Array[Byte]) = new String(k, "UTF-8")

}

class KvStoreHashListHandler extends HashKvStoreHandler[Vector[(Hash, Array[Byte])]] with HashListHandler {
  lazy val fileType: FileType[Vector[(Hash, Array[Byte])]] = fileManager.hashlists

  def addHashlist(fileHash: Hash, hashList: Array[Byte]): Unit = {
    ensureLoaded()
    if (isPersisted(fileHash))
      return
    writeEntry(fileHash, hashList.wrap())
  }

  def getHashlist(fileHash: Hash, size: Long): Seq[Hash] = {
    readEntry(fileHash)._1.asArray().grouped(config.hashLength).toSeq.map(new Hash(_))
  }

  def fileFinished() {
    universe.journalHandler().finishedFile(currentlyWritingFile.file, fileType)
  }
}

class KvStoreBlockHandler extends HashKvStoreHandler[Volume] with BlockHandler with Utils {
  lazy val fileType: FileType[Volume] = fileManager.volumes

  private val byteCounter = new MaxValueCounter() {
    var compressedBytes = 0L

    def name: String = "Blocks written"

    def r(x: Long): String = Utils.readableFileSize(x)

    override def formatted = s"${r(current)}/${r(maxValue)} (compressed ${r(compressedBytes)})"
  }

  private val compressionRatioCounter = new MaxValueCounter() {
    def name: String = "Compression Ratio"

    override def formatted: String = percent + "%"
  }

  ProgressReporters.addCounter(byteCounter, compressionRatioCounter)


  def fileFinished() {
    universe.journalHandler.finishedFile(currentlyWritingFile.file, fileType)
    createIndex()
  }

  override def readBlockAsync(hash: Hash): Future[BytesWrapper] = {
    Future.successful {
      readBlock(hash)
    }
  }

  override def readBlock(hash: Hash): BytesWrapper = {
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

  private var outstandingRequests: ByteArrayMap[Int] = new ByteArrayMap[Int]()

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
      val sizeAfterWriting = currentlyWritingFile.length() + block.compressed.length + config.hashLength + 50
      if (sizeAfterWriting > config.volumeSize.bytes) {
        currentlyWritingFile.close()
        fileFinished()
        currentlyWritingFile = null
      }
    }
    writeEntry(block.hash, block.compressed)
    byteCounter += outstandingRequests(block.hash)
    compressionRatioCounter.maxValue += outstandingRequests(block.hash)
    outstandingRequests -= block.hash
    byteCounter.compressedBytes += block.compressed.length
    compressionRatioCounter += block.compressed.length
  }

  def createIndex(): Unit = {
    if (config.createIndexes) {
      val indexFile = fileManager.volumeIndex.indexForFile(currentlyWritingFile.file)
      indexFile.getParentFile.mkdirs()
      val indexWriter = new KvStoreStorageMechanismWriter(indexFile, config.passphrase)
      indexWriter.setup(universe)
      indexWriter.writeManifest()
      val json = new JsonSerialization(indent = false)
      val indexList = persistedEntries.toVector.filter(_._2.file == currentlyWritingFile.file).sortBy(_._2.pos).map {
        case (k, v) =>
          (k, v.pos)
      }
      val jsonList = json.write(indexList)
      val compressed = CompressedStream.compress(jsonList.wrap, CompressionMode.deflate)
      indexWriter.add("list".getBytes, compressed)
      indexWriter.close()
      universe.journalHandler.finishedFile(indexFile, fileManager.volumeIndex)
    }
  }

  override def loadFile(file: File): Unit = {
    val indexFile = universe.fileManager().volumeIndex.indexForFile(file)
    if (indexFile.exists()) {
      val reader = new KvStoreStorageMechanismReader(indexFile, config.passphrase)
      val key = "list".getBytes()
      val json = new JsonSerialization()
      reader.iterator.filter(_._1 === key).foreach { case (k, loc) =>
        val value = reader.get(loc)
        val decomp = CompressedStream.decompressToBytes(value)
        json.read[Vector[(Array[Byte], Long)]](decomp) match {
          case Left(vec) =>
            val newEntries = vec.map { case (hash, pos) =>
              (storageToKeyMem(hash), KvStoreLocation(file, pos))
            }
            persistedEntries ++= newEntries
          case _ =>
            super.loadFile(file)
        }
      }
      reader.close()
    } else {
      super.loadFile(file)
    }
  }
}