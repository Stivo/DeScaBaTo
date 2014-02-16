package ch.descabato.core

import java.util.Date
import ch.descabato.utils.{ZipFileHandlerFactory, ObjectPools, Utils}
import java.util.BitSet
import scala.collection.mutable
import ch.descabato.utils.Implicits._
import java.io.File

class ZipBackupPartHandler extends BackupPartHandler with UniversePart with Utils with BackupProgressReporting {
  protected var unfinished: Map[String, FileDescriptionWrapper] = Map.empty

  override val filecountername = "Files hashed"
  override val bytecountername = "Data hashed"

  override def setupInternal() {
    universe.eventBus().subscribe(eventListener)
  }

  def nameOfOperation = "Should not be shown, bug here"

  // Doesn't know which backup to load, load is explicit here.
  def load() {}
  
  val eventListener: PartialFunction[BackupEvent, Unit] = {
    case HashListCheckpointed(set, blocks) =>
      universe.backupPartHandler().checkpoint(Some(set, blocks))
  }

  var current: BackupDescription = new BackupDescription()
  
  var toCheckpoint: BackupDescription = new BackupDescription()

  var failedObjects = new BackupDescription()
  
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
        toCheckpoint = toCheckpoint + fd
        if (hashList.length > fd.hash.length)
          universe.hashListHandler.addHashlist(fd.hash, hashList)
        fileCounter += 1
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

  // reads all the backup parts for the given date
  def loadBackup(date: Option[Date]): BackupDescription = {
    val filesToLoad: Seq[File] = fileManager.getLastBackup(temp = true) 
    current = filesToLoad
      .flatMap(fileManager.backup.read(_, OnFailureTryRepair))
      .fold(new BackupDescription())((x, y) => x.merge(y))
    current
  }

  // add files to the current backup description
  // Also may contain deleted files, which then should be removed from current
  def setFiles(bd: BackupDescription) {
    current = bd
    bd.files.foreach {
      file =>
        if (file.hash == null)
          getUnfinished(file)
    }
    setMaximums(bd)
    toCheckpoint = current.copy(files = current.files.filter(_.hash != null))
    l.info("After setCurrent " + remaining + " remaining")
  }

  protected def getUnfinished(fd: FileDescription) =
    unfinished.get(fd.path) match {
      case Some(w) => w
      case None =>
        val out = new FileDescriptionWrapper(fd)
        unfinished += fd.path -> out
        out
    }

  def hashForFile(fd: FileDescription, hash: Array[Byte]) {
    getUnfinished(fd).fileHashArrived(hash)
  }

  def fileFailed(fd: FileDescription) {
    getUnfinished(fd).setFailed()
  }

  // If file is complete, send hash list to the hashlist handler and mark
  // filedescription ready to be checkpointed
  def hashComputed(blockId: BlockId, hash: Array[Byte], content: Array[Byte]) {
    // TODO compressDisabled!
    universe.blockHandler.writeBlockIfNotExists(blockId, hash, content, false)
    byteCounter += content.length
    getUnfinished(blockId.file).blockHashArrived(blockId, hash)
  }

  def checkpoint(t: Option[(Set[BAWrapper2], Set[BAWrapper2])]) {
    if (toCheckpoint.isEmpty)
      return
    val (hashlists, blocks) = t match {
      case Some(x) => x
      case _ => (universe.hashListHandler.getAllPersistedKeys, universe.blockHandler.getAllPersistedKeys)
    }
    def isFinished(fd: FileDescription) = fd match {
      case fd: FileDescription if fd.hash == null => false
      case fd: FileDescription if fd.size <= config.blockSize.bytes =>
//        if (logMoreInfos && !universe.blockHandler().isPersisted(fd.hash))
//          l.info("File "+fd+" is not finished apparently "+Utils.encodeBase64Url(fd.hash))
        blocks safeContains fd.hash
      case fd: FileDescription =>
        hashlists safeContains fd.hash
    }
    val (filesToSave, filesToKeep) = toCheckpoint.files.partition(isFinished)
    val toSave = new BackupDescription(filesToSave, toCheckpoint.folders, toCheckpoint.symlinks, toCheckpoint.deleted)
    toCheckpoint = new BackupDescription(files = filesToKeep)
    if (!toSave.isEmpty) {
      fileManager.backup.write(toSave, true, true)
      l.info(s"Checkpointed ${toSave.size} items in file "+(fileManager.backup.nextNum(true) - 1))
    }
  }

  def remaining(): Int = unfinished.size

//  var logMoreInfos = false

  // write current, clear checkpoint files
  def finish() = {
//    logMoreInfos = true
    if (remaining != 0) {
      throw new IllegalStateException("Backup Part Handler must be finished before finish may be called")
    }
    checkpoint(None)
    fileManager.backup.mergeTempFilesIntoNew()
    if (!toCheckpoint.isEmpty()) {
      for (x <- toCheckpoint.asMap) {
        l.info("Still contains "+x)
      }
      throw new IllegalStateException("Did not write everything")
    }
    //fileManager.backup.write(current, false, true)
    true
  }
  
  def shutdown() = ret
  
}
