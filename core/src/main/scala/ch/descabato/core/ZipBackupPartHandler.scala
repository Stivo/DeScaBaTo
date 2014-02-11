package ch.descabato.core

import java.util.Date
import ch.descabato.utils.ObjectPools
import ch.descabato.utils.Utils
import java.util.BitSet

class ZipBackupPartHandler extends BackupPartHandler with UniversePart with Utils with BackupProgressReporting {
  protected var current: BackupDescription = null
  protected var unfinished: Map[String, FileDescriptionWrapper] = Map.empty

  override val filecountername = "Files hashed"
  override val bytecountername = "Data hashed"

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
  // Does not keep a copy of this data
  def readBackup(date: Option[Date]): BackupDescription = {
    fileManager.backup.getFiles().sortBy(_.getName).reverse.take(1)
      .flatMap(fileManager.backup.read(_, OnFailureTryRepair))
      .fold(new BackupDescription())((x, y) => x.merge(y))
  }

  // set current backup description, these are ready to be checkpointed
  // once the files are all in
  def setCurrent(bd: BackupDescription) {
    current = bd
    bd.files.foreach {
      file =>
        if (file.hash == null)
          getUnfinished(file)
    }
    setMaximums(bd)
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

  // Checkpoint right now, clear all that can be checkpointed from toCheckpoint
  def checkpoint(): Boolean = {
    // TODO
    true
  }

  def remaining(): Int = unfinished.size

  // write current, clear checkpoint files
  def finish() = {
    if (current != null)
      fileManager.backup.write(current)
    true
  }
}
