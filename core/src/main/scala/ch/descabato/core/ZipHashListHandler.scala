package ch.descabato.core

import scala.collection.mutable
import java.io.File
import scala.collection.mutable.Buffer
import ch.descabato.utils.Implicits._
import ch.descabato.utils.{Utils, ZipFileReader, ZipFileWriter}
import java.util.zip.ZipEntry

@Deprecated
class ZipHashListHandler extends HashListHandler {
  type HashListMap = mutable.HashMap[BAWrapper2, Array[Byte]]

  var hashListMap: HashListMap = new HashListMap()
  var hashListMapNew: HashListMap = new HashListMap()
  var hashListMapCheckpoint: HashListMap = new HashListMap()

  def load() { /* already done */ } 
  def shutdown() = { finish; ret }
  
  override def setupInternal() {
    hashListMap ++= oldBackupHashLists
  }

  def oldBackupHashLists: mutable.Map[BAWrapper2, Array[Byte]] = {
    def loadFor(x: Iterable[File], failureOption: ReadFailureOption) = {
      x.flatMap(x => fileManager.hashlists.read(x, failureOption)).fold(Buffer())(_ ++ _).toBuffer
    }
    val temps = loadFor(fileManager.hashlists.getTempFiles(), OnFailureDelete)
    if (!temps.isEmpty) {
      fileManager.hashlists.write(temps)
      fileManager.hashlists.deleteTempFiles()
    }
    val list = loadFor(fileManager.hashlists.getFiles(), OnFailureTryRepair)
    val map = new HashListMap
    map ++= list
    map
  }

  def hashListSeq(a: Array[Byte]) = a.grouped(config.getMessageDigest.getDigestLength()).toSeq

  def getHashlist(fileHash: Array[Byte], size: Long): Seq[Array[Byte]] = {
    if (size <= config.blockSize.bytes) {
      List(fileHash)
    } else {
      hashListSeq(hashListMap.get(fileHash).get)
    }
  }

  def addHashlist(fileHash: Array[Byte], hashList: Array[Byte]) {
    if (!(hashListMap safeContains fileHash))
      hashListMapNew += ((fileHash, hashList))
  }
  
  def checkpoint() = {
    // TODO!!!!!
    true
  }
  
  def getAllPersistedKeys(): Set[BAWrapper2] = hashListMap.keySet.toSet
  
  def finish() = {
    if (!hashListMapNew.isEmpty)
      fileManager.hashlists.write(hashListMapNew.toBuffer)
    fileManager.hashlists.deleteTempFiles()
    true
  }

  override def isPersisted(fileHash: Array[Byte]) = {
    hashListMap safeContains fileHash
  }

  // Checkpointing with an optional argument
  override def checkpoint(t: Option[Set[BAWrapper2]]): Unit = ???
}

class NewZipHashListHandler extends StandardZipKeyValueStorage with HashListHandler with UniversePart with Utils {
  override val folder = "hashlists/"

  override val writeTempFiles = true

  def filetype = fileManager.hashlists

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

  def shouldStartNextFile(w: ZipFileWriter, k: BAWrapper2, v: Array[Byte]) = false

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
      val num = filetype.num(file)
      super.endZipFile()
      l.info("Wrote hashlist "+num)
    }
  }

  override def finish() = {
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