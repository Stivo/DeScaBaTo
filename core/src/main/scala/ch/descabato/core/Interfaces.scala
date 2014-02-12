package ch.descabato.core

import ch.descabato.CompressionMode
import java.io.{File, InputStream}
import java.util.Date
import java.nio.ByteBuffer
import java.util.zip.ZipEntry
import ch.descabato.utils.Streams
import java.security.MessageDigest

trait MustFinish {
  // Return value is used so akka blocks this call
  // Return value can signal failure, but currently no checks are done
  def finish(): Boolean
}

trait CanVerify {
  def verify(counter: ProblemCounter): Boolean
}

trait Universe extends MustFinish {
  def config(): BackupFolderConfiguration
  def backupPartHandler(): BackupPartHandler
  def cpuTaskHandler(): CpuTaskHandler
  def hashListHandler(): HashListHandler
  def blockHandler(): BlockHandler
  def hashHandler(): HashHandler
  def journalHandler(): JournalHandler
  def redundancyHandler(): RedundancyHandler = {
    if (false) {
      makeRedundancyHandler()
    } else {
      new NoOpRedundancyHandler()
    }
  }
  protected def makeRedundancyHandler(): RedundancyHandler = ???
  def compressionStatistics(): Option[CompressionStatistics] = None

  lazy val _fileManager = new FileManager(this)
  def fileManager() = _fileManager

  def waitForQueues() {}
  def finish(): Boolean
  def shutdown() {
    compressionStatistics().foreach{ _.report }
  }
}

trait JournalHandler extends UniversePart with MustFinish {
  // Saves this file in the journal as finished
  // Makes an effort to sync it to the file system
  def finishedFile(name: String, filetype: FileType[_]): Boolean
  // Removes all files not mentioned in the journal
  def cleanUnfinishedFiles(): Boolean
}

case class BlockId(file: FileDescription, part: Int)

trait BackupActor extends MustFinish {
  def setup(universe: Universe)
}

// TODO implement this again? or design it better?
trait RedundancyHandler extends UniversePart with MustFinish {
  // Requires that the file is finished
  def createPar2(file: File)
  // Requires that the file is not in use
  def tryRepair(file: File): Boolean
  // No requirements, will just return None if no hash exists
  def md5HashForFile(file: File): Option[Array[Byte]]
  // Will just return same stream if no md5 hash around
  def wrapVerifyStreamIfCovered(file: File, is: InputStream): InputStream = {
    md5HashForFile(file) match {
      case Some(hash) => new Streams.VerifyInputStream(is, MessageDigest.getInstance("MD5"), hash, f)
      case None => is
    }
  }
}

class NoOpRedundancyHandler extends UniversePart with MustFinish {
  def createPar2(file: File) {}
  def tryRepair(file: File) = false
  def md5HashForFile(file: File): Option[Array[Byte]] = None
  def finish() = true
}

trait CompressionStatistics extends UniversePart {
  def blockStatistics(block: BlockId, compressedSize: Long, uncompressedSize: Long, method: CompressionMode, time: Long)
  def report()
}

trait BackupPartHandler extends BackupActor {
  // reads all the backup parts for the given date
  def readBackup(date: Option[Date] = None): BackupDescription

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

trait HashListHandler extends BackupActor with UniversePart {
  def addHashlist(fileHash: Array[Byte], hashList: Array[Byte])
  def getHashlist(fileHash: Array[Byte], size: Long): Seq[Array[Byte]]
  def checkpoint(): Boolean
}

trait MetadataHandler extends HashListHandler with BackupPartHandler

trait BlockHandler extends BackupActor with UniversePart with CanVerify {
  def writeBlockIfNotExists(blockId: BlockId, hash: Array[Byte], block: Array[Byte], compressDisabled: Boolean)
  // or multiple blocks
  def isPersisted(hash: Array[Byte]): Boolean
  def readBlock(hash: Array[Byte], verify: Boolean): InputStream
  def writeCompressedBlock(hash: Array[Byte], zipEntry: ZipEntry, header: Byte, block: ByteBuffer)
  def remaining: Int
  def setTotalSize(size: Long)
}

trait CpuTaskHandler extends MustFinish {
  // calls then hashComputed on metadatawriter
  def computeHash(x: Array[Byte], hashMethod: String, blockId: BlockId)
  // Calls then blockhandler#writeBlockIfNotExists
  def compress(blockId: BlockId, hash: Array[Byte], content: Array[Byte], method: CompressionMode, disable: Boolean)
}

trait HashHandler {
    // Hash this block of this file
  def hash(blockId: BlockId, block: Array[Byte])
  // Finish this file, call backupparthandler 
  def finish(fd: FileDescription)
  // Release resources associated with this file
  def fileFailed(fd: FileDescription)
}