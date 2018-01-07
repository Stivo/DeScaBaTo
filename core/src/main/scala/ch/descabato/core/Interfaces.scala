package ch.descabato.core

import java.io.File
import java.util.Date

import ch.descabato.frontend.MaxValueCounter
import ch.descabato.remote.{BackupPath, RemoteFile, RemoteOperation}
import ch.descabato.utils._

import scala.concurrent.Future
import scala.util.Try

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

  def loadBlocking(): BlockingOperation = {
    load()
    new BlockingOperation()
  }

  // Releases all the resources in use
  def shutdown(): BlockingOperation
}

trait PureLifeCycle extends LifeCycle {
  def load() {}

  def shutdown(): BlockingOperation = ret

  def finish() = true
}

trait BackupActor extends UniversePart with LifeCycle

trait Universe extends LifeCycle {
  def config(): BackupFolderConfiguration

  def backupPartHandler(): BackupPartHandler

  def hashListHandler(): HashListHandler

  def blockHandler(): BlockHandler

  def hashFileHandler(): HashFileHandler

  def journalHandler(): JournalHandler

  def remoteHandler(): RemoteHandler

  def redundancyHandler(): RedundancyHandler = {
    new NoOpRedundancyHandler()
  }

  def compressionDecider(): CompressionDecider

  lazy val startUpOrder: List[LifeCycle] = List(journalHandler(),
    hashFileHandler, blockHandler, hashListHandler, backupPartHandler, remoteHandler)

  lazy val finishOrder = List(blockHandler, hashListHandler, backupPartHandler,
    hashFileHandler, remoteHandler, journalHandler)

  def createRestoreHandler(description: FileDescription, file: File, filecounter: MaxValueCounter): RestoreFileHandler

  // Doesn't really matter
  lazy val shutdownOrder: List[LifeCycle] = startUpOrder.reverse

  lazy val _fileManager = new FileManager(this)

  def fileManager(): FileManager = _fileManager

  def scheduleTask[T](f: () => T): Future[T]

  def waitForQueues() {}

  def finish(): Boolean

  def shutdown(): BlockingOperation = {
    compressionDecider().report
    ret
  }
}

trait JournalHandler extends BackupActor {
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

  override def load(): Unit = new IllegalAccessException("Journal handler must be loaded synchronously")

  override def loadBlocking(): BlockingOperation = cleanUnfinishedFiles

  def startWriting(): BlockingOperation

  def stopWriting(): BlockingOperation
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
  //  def wrapVerifyStreamIfCovered(file: File, is: InputStream): InputStream = {
  //    md5HashForFile(file) match {
  //      case Some(hash) => new Streams.VerifyInputStream(is, MessageDigest.getInstance("MD5"), hash, file)
  //      case None => is
  //    }
  //  }
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

  def loadedBackup: Seq[File]

  // reads all the backup parts for the given date
  def loadBackup(date: Option[Date] = None): BackupDescription

  // sets the backup description that should be saved
  def setFiles(finishedFiles: BackupDescription, unfinishedFiles: BackupDescription)

  // sets the hash for this file
  def hashForFile(fd: FileDescription, hash: Hash)

  // Sets this file as failed
  def fileFailed(fd: FileDescription)

  // If file is complete, send hash list to the hashlist handler and mark
  // filedescription ready to be checkpointed
  def hashComputed(blockWrapper: Block)

  def remaining(): Int
}

trait HashListHandler extends BackupActor {
  def addHashlist(fileHash: Hash, hashList: Array[Byte])

  def getHashlist(fileHash: Hash, size: Long): Seq[Hash]

  def isPersisted(fileHash: Hash): Boolean
}

trait BlockHandler extends BackupActor with CanVerify {
  def writeBlockIfNotExists(blockWrapper: Block)

  def isPersisted(hash: Hash): Boolean

  def readBlockAsync(hash: Hash): Future[BytesWrapper]

  def readBlock(hash: Hash): BytesWrapper

  def writeCompressedBlock(blockWrapper: Block)

  def remaining: Int

  def setTotalSize(size: Long)
}

trait RemoteHandler extends BackupActor {
  def remaining(): Int

  def uploadFile(file: File)

  def startUploading()

  def stopUploading()

  def read(backupPath: BackupPath): Future[Unit]

  def getFiles(fileType: FileType[_]): List[RemoteFile]

  // Internal, do not call from outside
  def fileOperationFinished(operation: RemoteOperation, result: Try[Unit])
}

trait HashHandler extends BackupActor with PureLifeCycle {
  def hash(bytes: BytesWrapper)

  def finish(f: Hash => Unit)
}

trait HashFileHandler extends BackupActor {
  // Hash this block of this file
  def hash(blockwrapper: Block)

  // Finish this file, call backupparthandler
  def finish(fd: FileDescription)

  // Release resources associated with this file
  def fileFailed(fd: FileDescription)

  def remaining(): Int = 0
}

trait RestoreFileHandler extends UniversePart {
  def restore(): Future[Boolean]
}