package ch.descabato.core

import ch.descabato.CompressionMode
import java.io.InputStream
import java.util.Date
import java.nio.ByteBuffer
import java.util.zip.ZipEntry

trait Universe {
  def config(): BackupFolderConfiguration
  def backupPartHandler(): BackupPartHandler
  def cpuTaskHandler(): CpuTaskHandler
  def hashListHandler(): HashListHandler
  def blockHandler(): BlockHandler
  def hashHandler(): HashHandler

  lazy val _fileManager = new FileManager(config)
  def fileManager() = _fileManager

  def waitForQueues() {}
  def finish() {}
  def shutdown() {}
}

case class BlockId(file: FileDescription, part: Int)

trait BackupActor {
  def setup(universe: Universe)
  def finish(): Boolean
}

trait BackupPartHandler extends BackupActor {
  // reads all the backup parts for the given date
  def readBackup(date: Option[Date]): BackupDescription

  // sets the backup description that should be saved
  def setCurrent(bd: BackupDescription)
  
  // sets the hash for this file
  def hashForFile(fd: FileDescription, hash: Array[Byte])
  
  // If file is complete, send hash list to the hashlist handler and mark
  // filedescription ready to be checkpointed
  def hashComputed(blockId: BlockId, hash: Array[Byte], content: Array[Byte])
  
  // Checkpoint right now
  def checkpoint(): Boolean
  
  def remaining(): Int
}

trait HashListHandler extends BackupActor {
  def addHashlist(fileHash: Array[Byte], hashList: Array[Byte])
  def getHashlist(fileHash: Array[Byte], size: Long): Seq[Array[Byte]]
  def checkpoint(): Boolean
}

trait MetadataHandler extends HashListHandler with BackupPartHandler

trait BlockHandler extends BackupActor {
  def writeBlockIfNotExists(hash: Array[Byte], block: Array[Byte], compressDisabled: Boolean)
  // or multiple blocks
  def blockIsPersisted(hash: Array[Byte]): Boolean
  def readBlock(hash: Array[Byte], verify: Boolean): InputStream
  def writeCompressedBlock(hash: Array[Byte], zipEntry: ZipEntry, header: Byte, block: ByteBuffer)
  def remaining: Int
  def setTotalSize(size: Long)
}

trait CpuTaskHandler {
  def finish(): Boolean
  // calls then hashComputed on metadatawriter
  def computeHash(x: Array[Byte], hashMethod: String, blockId: BlockId)
  // Calls then blockhandler#writeBlockIfNotExists
  def compress(hash: Array[Byte], content: Array[Byte], method: CompressionMode, disable: Boolean)
}

trait HashHandler {
    // Hash this block of this file
  def hash(blockId: BlockId, block: Array[Byte])
  // Finish this file, call backupparthandler 
  def finish(fd: FileDescription)
}