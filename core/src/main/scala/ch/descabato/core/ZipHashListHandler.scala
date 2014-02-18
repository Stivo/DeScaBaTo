package ch.descabato.core

import java.io.File
import scala.collection.mutable.Buffer
import ch.descabato.utils.Implicits._
import ch.descabato.utils.{Utils, ZipFileReader, ZipFileWriter}
import java.util.zip.ZipEntry

class ZipHashListHandler extends HashListHandler with Utils {
  type HashListMap = Map[BAWrapper2, Array[Byte]]

  var hashListsPersisted: HashListMap = Map.empty
  var hashListsToWrite: HashListMap = Map.empty

  def load() {

  }

  override def setupInternal() {
    universe.eventBus().subscribe(eventListener)
    hashListsPersisted = oldBackupHashLists
  }

  val eventListener: PartialFunction[BackupEvent, Unit] = {
    case e @ VolumeFinished(file, blocks) =>
      // If this is in akka, thread safety is only possible to get via the actor reference
      universe.hashListHandler().checkpoint(Some(blocks))
  }

  def shutdown() = { finish; ret }
  
  def oldBackupHashLists: Map[BAWrapper2, Array[Byte]] = {
    def loadFor(x: Iterable[File], failureOption: ReadFailureOption) = {
      x.flatMap(x => fileManager.hashlists.read(x, failureOption)).fold(Vector())(_ ++ _)
    }
    val temps = loadFor(fileManager.hashlists.getTempFiles(), OnFailureDelete)
    val list = loadFor(fileManager.hashlists.getFiles(), OnFailureTryRepair)
    var map: HashListMap = Map.empty
    map ++= list
    map ++= temps
    map
  }

  def hashListSeq(a: Array[Byte]) = a.grouped(config.getMessageDigest.getDigestLength()).toSeq

  def getHashlist(fileHash: Array[Byte], size: Long): Seq[Array[Byte]] = {
    if (size <= config.blockSize.bytes) {
      List(fileHash)
    } else {
      hashListSeq(hashListsPersisted.get(fileHash).get)
    }
  }

  def addHashlist(fileHash: Array[Byte], hashList: Array[Byte]) {
    if (!(hashListsPersisted safeContains fileHash) && !(hashListsToWrite safeContains fileHash))
      hashListsToWrite += ((fileHash, hashList))
  }
  
  def getAllPersistedKeys(): Set[BAWrapper2] = hashListsPersisted.keySet
  
  def finish() = {
    if (!hashListsToWrite.isEmpty)
      checkpoint(None)
    fileManager.hashlists.mergeTempFilesIntoNew()
    true
  }

  override def isPersisted(fileHash: Array[Byte]) = {
    hashListsPersisted safeContains fileHash
  }

  // Checkpointing with an optional argument
  def checkpoint(blocks: Option[Set[BAWrapper2]]) {
    val persistedBlocks = blocks match {
      case Some(set) => set
      case None => universe.blockHandler.getAllPersistedKeys
    }
    def allBlocksExist(x: Array[Byte]) = hashListSeq(x).forall(persistedBlocks(_))
    val (toWrite, toKeep) = hashListsToWrite.partition{case (_, value) => allBlocksExist(value)}
    if (!toWrite.isEmpty) {
      val num = fileManager.hashlists.nextNum(true)
      fileManager.hashlists.write(toWrite.toVector, true)
      l.info("Wrote temp hashlist "+num)
      hashListsPersisted ++= toWrite
      hashListsToWrite = toKeep
      universe.eventBus().publish(HashListCheckpointed(getAllPersistedKeys, persistedBlocks))
    }
  }
}

// Index capability needs to be added
@Deprecated
class NewZipHashListHandler extends StandardZipKeyValueStorage with HashListHandler with UniversePart with Utils {
  override val folder = "hashlists/"

  def indexFileType = ???
    
  override val writeTempFiles = true

  def filetype = fileManager.hashlists

  override val keepValuesInMemory = true

  override def load() { super.load }
  override def shutdown() = { finish; ret }
  
  var hashListsToWrite = Map[BAWrapper2, Array[Byte]]()

  override def setupInternal() {
    universe.eventBus().subscribe(eventListener)
  }

  val eventListener: PartialFunction[BackupEvent, Unit] = {
    case e @ VolumeFinished(file, blocks) =>
      // If this is in akka, thread safety is only possible to get via the actor reference
      universe.hashListHandler().checkpoint(Some(blocks))
  }

  override def configureWriter(writer: ZipFileWriter) {
    writer.enableCompression()
  }

  def addHashlist(fileHash: Array[Byte], hashList: Array[Byte]) {
    if (!exists(fileHash)) {
      hashListsToWrite += ((fileHash, hashList))
    }
  }

  def hashListSeq(a: Array[Byte]) = a.grouped(config.getMessageDigest.getDigestLength()).toSeq

  def getHashlist(fileHash: Array[Byte], size: Long) ={
    hashListSeq(read(fileHash))
  }

  def shouldStartNextFile(w: ZipFileWriter, k: BAWrapper2, v: Array[Byte]) = hashListsToWrite.size > 10000

  def checkpoint(blocks: Option[Set[BAWrapper2]]) {
    val persistedBlocks = blocks match {
      case Some(set) => set
      case None => universe.blockHandler.getAllPersistedKeys
    }
    def allBlocksExist(x: Array[Byte]) = hashListSeq(x).forall(persistedBlocks(_))
    val (toWrite, toKeep) = hashListsToWrite.partition{case (_, value) => allBlocksExist(value)}
    if (!toWrite.isEmpty) {
      toWrite.foreach {case (k, v) => write(k, v)}
      endZipFile()
      hashListsToWrite = toKeep
      universe.eventBus().publish(HashListCheckpointed(getAllPersistedKeys, persistedBlocks))
    }
  }

  def getAllPersistedKeys(): Set[BAWrapper2] = inBackupIndex.keySet
  
  override def endZipFile() {
    if (currentWriter != null) {
      val file = currentWriter.file
      val num = filetype.numberOf(file)
      super.endZipFile()
      l.info("Wrote hashlist "+num)
    }
  }

  override def finish() = {
    if (!hashListsToWrite.isEmpty)
      checkpoint(None)
    super.finish()
    val temps = filetype.getTempFiles()
    if (!temps.isEmpty) {
      super.mergeFiles(temps, filetype.nextFile(temp = false))
      filetype.deleteTempFiles()
    }
    true
  }

}