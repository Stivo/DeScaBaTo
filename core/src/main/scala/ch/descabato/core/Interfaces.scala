package ch.descabato.core

import java.io.{File, InputStream}
import java.nio.ByteBuffer
import java.security.MessageDigest
import java.util.Date

import ch.descabato.CompressionMode
import ch.descabato.utils.Implicits._
import ch.descabato.utils.{ObjectPools, Streams}

import scala.concurrent.Future

trait MustFinish {
  // Finishes writing, so the backup can be completed
  // Resources for reading are still kept
  def finish(): Boolean
}

trait CanVerify {
  def verify(counter: ProblemCounter): Boolean
}

trait LifeCycle extends MustFinish {
  def ret = new BlockingOperation
  def mayUseNonBlockingLoad = true
  // Loads the data associated with this component
  def load()
  def loadBlocking() = {
    load()
    new BlockingOperation()
  }
  // Releases all the resources in use
  def shutdown(): BlockingOperation
}

trait PureLifeCycle extends LifeCycle {
  def load() {}
  def shutdown() = ret
  def finish() = true
}

trait Universe extends LifeCycle {
  def config(): BackupFolderConfiguration
  def backupPartHandler(): BackupPartHandler
  def hashListHandler(): HashListHandler
  def blockHandler(): BlockHandler
  def hashHandler(): HashHandler
  def journalHandler(): JournalHandler
  def redundancyHandler(): RedundancyHandler = {
      new NoOpRedundancyHandler()
  }
  def compressionDecider(): CompressionDecider

  lazy val startUpOrder: List[LifeCycle] = List(journalHandler(),
      hashHandler, blockHandler, hashListHandler, backupPartHandler)

  lazy val finishOrder = List(blockHandler, hashListHandler, backupPartHandler,
    hashHandler, journalHandler)

  // Doesn't really matter
  lazy val shutdownOrder = startUpOrder.reverse
      
  lazy val _fileManager = new FileManager(this)
  def fileManager() = _fileManager

  def scheduleTask[T](f: () => T): Future[T]

  def waitForQueues() {}
  def finish(): Boolean
  def shutdown() = {
    compressionDecider().report
    ret
  }
}

trait JournalHandler extends UniversePart with LifeCycle {
  // Saves this file in the journal as finished
  // Makes an effort to sync it to the file system
  def finishedFile(file: File, filetype: FileType[_], journalUpdate: Boolean = false): BlockingOperation
  // Removes all files not mentioned in the journal
  def cleanUnfinishedFiles(): BlockingOperation
  // All the identifiers that were once used and are now unsafe to use again
  def usedIdentifiers(): Set[String]

  // Reports whether the last shut down was dirty (crash)
  def isInconsistentBackup(): Boolean
  
  override val mayUseNonBlockingLoad = false
  override def load() = new IllegalAccessException("Journal handler must be loaded synchronously")
  override def loadBlocking() = cleanUnfinishedFiles
  def startWriting(): BlockingOperation
  def stopWriting(): BlockingOperation
}

case class BlockId(file: FileDescription, part: Int)

class Block(val id: BlockId, val content: Array[Byte]) {
  val uncompressedLength = content.length 
  @volatile var hash: Array[Byte] = null
  @volatile var mode: CompressionMode = null
  @volatile var compressed: ByteBuffer = null
  def recycle() {
    if (compressed != null && compressed.array() != content)
      compressed.recycle()
    ObjectPools.byteArrayPool.recycle(content)
  }
}

trait BackupActor extends UniversePart with LifeCycle

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
      case Some(hash) => new Streams.VerifyInputStream(is, MessageDigest.getInstance("MD5"), hash, file)
      case None => is
    }
  }
}

class NoOpRedundancyHandler extends RedundancyHandler with PureLifeCycle {
  def createPar2(file: File) {}
  def tryRepair(file: File) = false
  def md5HashForFile(file: File): Option[Array[Byte]] = None
}

trait CompressionDecider extends UniversePart {
  def compressBlock(block: Block)
  def blockCompressed(block: Block, nanoTime: Long)
  def report()
}

trait BackupPartHandler extends BackupActor {

  // reads all the backup parts for the given date
  def loadBackup(date: Option[Date] = None): BackupDescription

  // sets the backup description that should be saved
  def setFiles(finishedFiles: BackupDescription, unfinishedFiles: BackupDescription)

  // sets the hash for this file
  def hashForFile(fd: FileDescription, hash: Array[Byte])

  // Sets this file as failed
  def fileFailed(fd: FileDescription)

  // If file is complete, send hash list to the hashlist handler and mark
  // filedescription ready to be checkpointed
  def hashComputed(blockWrapper: Block)
  
  def remaining(): Int
}

trait HashListHandler extends BackupActor with UniversePart {
  def addHashlist(fileHash: Array[Byte], hashList: Array[Byte])
  def getHashlist(fileHash: Array[Byte], size: Long): Seq[Array[Byte]]
  def getAllPersistedKeys(): Set[BaWrapper]
  def isPersisted(fileHash: Array[Byte]) = getAllPersistedKeys safeContains fileHash
}

trait BlockHandler extends BackupActor with UniversePart with CanVerify {
  def writeBlockIfNotExists(blockWrapper: Block)
  def getAllPersistedKeys(): Set[BaWrapper]
  def isPersisted(hash: Array[Byte]): Boolean
  def readBlock(hash: Array[Byte], verify: Boolean): InputStream
  def writeCompressedBlock(blockWrapper: Block)
  def remaining: Int
  def setTotalSize(size: Long)
}

trait HashHandler extends LifeCycle with UniversePart {
    // Hash this block of this file
  def hash(blockwrapper: Block)
  // Finish this file, call backupparthandler 
  def finish(fd: FileDescription)
  // Release resources associated with this file
  def fileFailed(fd: FileDescription)

  def remaining(): Int = 0
}
