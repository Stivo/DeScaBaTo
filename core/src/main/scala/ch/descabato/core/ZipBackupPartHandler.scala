package ch.descabato.core

import java.util.Date
import ch.descabato.utils.{ZipFileHandlerFactory, ObjectPools, Utils}
import java.util.BitSet
import scala.collection.mutable
import ch.descabato.utils.Implicits._
import java.io.File

class ZipBackupPartHandler extends BackupPartHandler with UniversePart with Utils with BackupProgressReporting {
  protected var current: BackupDescription = null
  protected var unfinished: Map[String, FileDescriptionWrapper] = Map.empty

  override val filecountername = "Files hashed"
  override val bytecountername = "Data hashed"

  override def setupInternal() {
    universe.eventBus().subscribe(eventListener)
  }

  val eventListener: PartialFunction[BackupEvent, Unit] = {
    case HashListCheckpointed(set) =>
      universe.backupPartHandler().checkpoint(Some(set))
  }

  var toCheckpoint: BackupDescription = new BackupDescription()

  def blocksFor(fd: FileDescription) = {
    if (fd.size == 0) 1
    else (1.0 * fd.size / config.blockSize.bytes).ceil.toInt
  }

  class FileDescriptionWrapper(val fd: FileDescription) {
    lazy val blocks: BitSet = {
      val out = new BitSet(blocksFor(fd))
      out.set(0, blocksFor(fd), true)
      out
    }
    var failed = false
    var hashList = ObjectPools.byteArrayPool.getExactly(blocksFor(fd) * config.hashLength)

    def setFailed() {
      failed = true
      ObjectPools.byteArrayPool.recycle(hashList)
      hashList = null
    }

    def fileHashArrived(hash: Array[Byte]) {
      fd.hash = hash
      checkFinished()
    }

    private def checkFinished() {
      if (blocks.isEmpty && fd.hash != null && !failed) {
        unfinished -= fd.path
        if (hashList.length > fd.hash.length)
          universe.hashListHandler.addHashlist(fd.hash, hashList)
        fileCounter += 1
        updateProgress
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
    val filesToLoad: Seq[File] = fileManager.backup.getTempFiles() ++ fileManager.backup.getFiles().lastOption
    current = filesToLoad
      .flatMap(fileManager.backup.read(_, OnFailureTryRepair))
      .fold(new BackupDescription())((x, y) => x.merge(y))
    current
  }

  // add files to the current backup description
  // Also may contain deleted files, which then should be removed from current
  def addFiles(bd: BackupDescription) {
    current = current.merge(bd)
    bd.files.foreach {
      file =>
        if (file.hash == null)
        getUnfinished(file)
    }
    setMaximums(bd)
    toCheckpoint = current.copy(files = current.files.toList.toBuffer, folders = current.folders.toList.toBuffer,
      symlinks = current.symlinks.toList.toBuffer, deleted = mutable.Buffer.empty)
    universe.blockHandler.setTotalSize(byteCounter.maxValue)
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
    updateProgress
    getUnfinished(blockId.file).blockHashArrived(blockId, hash)
  }

  def checkpoint(t: Option[Set[BAWrapper2]]) {
    def lookup(x: Array[Byte]) = (t, x) match {
      case (Some(set), h) if set safeContains h => true
      case (_, h) => universe.hashListHandler().isPersisted(h)
    }
    def isFinished(x: UpdatePart) = x match {
      case fd: FileDescription if fd.hash == null => false
      case fd: FileDescription if fd.size <= config.blockSize.bytes => true
      case fd: FileDescription => lookup(fd.hash)
      case fd: FileDescription => throw new IllegalStateException("No other case should exist")
      case _ => true
    }
    val saveThisTime = new BackupDescription()
    def update[T <: UpdatePart](read: BackupDescription => mutable.Buffer[T], write: (mutable.Buffer[T], BackupDescription) => Unit) {
      val (finished, remain) = read(toCheckpoint).partition(isFinished)
      write(remain, toCheckpoint)
      write(finished, saveThisTime)
    }
    update[FileDescription](x => x.files, (b, o) => {o.files.clear; o.files ++= b})
    update[SymbolicLink](x => x.symlinks, (b, o) => {o.symlinks.clear; o.symlinks ++= b})
    update[FolderDescription](x => x.folders, (b, o) => {o.folders.clear; o.folders ++= b})
    update[FileDeleted](x => x.deleted, (b, o) => {o.deleted.clear; o.deleted ++= b})
    if (saveThisTime.size > 0)
      fileManager.backup.write(saveThisTime, true)
  }

  def remaining(): Int = unfinished.size

  // write current, clear checkpoint files
  def finish() = {
    checkpoint(None)
    val writer = ZipFileHandlerFactory.complexWriter(fileManager.backup.nextFile())
    fileManager.backup.mergeTempFilesIntoNew()
    true
  }
}
