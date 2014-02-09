package ch.descabato.core

import java.util.Date
import scala.collection.mutable.Buffer
import scala.collection.mutable
import ch.descabato.utils.ObjectPools
import scala.collection.mutable.HashMap
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
    lazy val hashList = ObjectPools.byteArrayPool.getExactly(blocksFor(fd) * config.hashLength)
  }

  // reads all the backup parts for the given date
  // Does not keep a copy of this data
  def readBackup(date: Option[Date]): BackupDescription = {
    fileManager.backup.getFiles().reverse.take(1)
    	.flatMap(fileManager.backup.read(_, OnFailureTryRepair))
    	.fold(new BackupDescription())((x, y) => x.merge(y))
  }
  
  // set current backup description, these are ready to be checkpointed
  // once the files are all in
  def setCurrent(bd: BackupDescription) {
    current = bd
    bd.files.foreach { file =>
      if (file.hash == null)
        getUnfinished(file)
    }
    setMaximums(bd)
    universe.blockHandler.setTotalSize(byteCounter.maxValue)
    l.info("After setCurrent "+remaining+" remaining")
  }

  protected def getUnfinished(fd: FileDescription) =
    unfinished.get(fd.path) match {
      case Some(w) => w
      case None =>
        val out = new FileDescriptionWrapper(fd)
        unfinished += fd.path -> out
        out
    }
  
  private def checkFinished(fd: FileDescription) {
    val w = getUnfinished(fd) 
    if (w.blocks.isEmpty && w.fd.hash != null) {
        unfinished -= fd.path
        if (w.hashList.length > fd.hash.length)
          universe.hashListHandler.addHashlist(fd.hash, w.hashList)
        fileCounter += 1
        updateProgress
    }
  }
  
  def hashForFile(fd: FileDescription, hash: Array[Byte]) {
    getUnfinished(fd).fd.hash = hash
    checkFinished(fd)
  }
  
  // If file is complete, send hash list to the hashlist handler and mark
  // filedescription ready to be checkpointed
  def hashComputed(blockId: BlockId, hash: Array[Byte], content: Array[Byte]) {
    // TODO compressDisabled!
    universe.blockHandler.writeBlockIfNotExists(hash, content, false)
    byteCounter += content.length
    updateProgress
    val w = getUnfinished(blockId.file)
    w.blocks.clear(blockId.part)
    System.arraycopy(hash, 0, w.hashList, blockId.part * config.hashLength, config.hashLength)
    checkFinished(w.fd)
  }
  
  // Checkpoint right now, clear all that can be checkpointed from toCheckpoint
  def checkpoint(): Boolean = {
    // TODO
    true
  }
  
  def remaining(): Int = unfinished.size
  
  // write current, clear checkpoint files
  def finish() = {
    fileManager.backup.write(current)
    true
  }
}
