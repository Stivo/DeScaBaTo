package ch.descabato.core

import java.io.File
import java.util.Date

import akka.actor.TypedActor
import ch.descabato.CompressionMode
import ch.descabato.core.actors.MetadataStorageActor.BackupDescription
import ch.descabato.core.actors.{BlockingOperation, JournalHandler, MyEventReceiver}
import ch.descabato.core.commands.ProblemCounter
import ch.descabato.core.config.BackupFolderConfiguration
import ch.descabato.core.model._
import ch.descabato.core.util.Json
import ch.descabato.utils.{BytesWrapper, CompressedStream, Hash}
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter

import scala.concurrent.Future
import scala.util.{Failure, Try}

trait ChunkIdResult
case object ChunkUnknown extends ChunkIdResult
case class ChunkIdAssigned(id: Long) extends ChunkIdResult
case class ChunkFound(id: Long) extends ChunkIdResult

trait ChunkStorage extends LifeCycle {

  def verifyChunksAreAvailable(chunkIdsToTest: Seq[Long], counter: ProblemCounter, checkVolumeToo: Boolean = true, checkContent: Boolean = false): BlockingOperation

  def read(id: Long): Future[BytesWrapper]

  def hasAlready(id: Long): Future[Boolean]

  def getHashForId(id: Long): Future[Hash]

  def chunkId(block: Block, assignIdIfNotFound: Boolean): Future[ChunkIdResult] = chunkId(block.hash, assignIdIfNotFound)

  def chunkId(hash: Hash, assignIdIfNotFound: Boolean): Future[ChunkIdResult]

  def save(block: CompressedBlock, id: Long): Future[Boolean]
}

sealed trait FileAlreadyBackedupResult

case object FileNotYetBackedUp extends FileAlreadyBackedupResult

case class FileAlreadyBackedUp(fileMetadata: FileMetadataStored) extends FileAlreadyBackedupResult

case object Storing extends FileAlreadyBackedupResult

trait MetadataStorage extends LifeCycle with TypedActor.PreRestart with MyEventReceiver {

  def getAllFileChunkIds(): Seq[Long]

  def verifyMetadataForIdsAvailable(date: Date, counter: ProblemCounter): BlockingOperation

  def retrieveBackup(date: Option[Date] = None): Future[BackupDescription]

  def addDirectory(description: FolderDescription): Future[Boolean]

  def getKnownFiles(): Map[String, FileMetadataStored]

  def hasAlready(fileDescription: FileDescription): Future[FileAlreadyBackedupResult]

  def saveFile(fileDescription: FileDescription, hashes: Seq[Long]): Future[Boolean]

  def saveFileSameAsBefore(fileMetadataStored: FileMetadataStored): Future[Boolean]
}

trait JsonUser {
  def config: BackupFolderConfiguration

  def journalHandler: JournalHandler

  def readJson[T: Manifest](file: File): Try[T] = {
    val reader = config.newReader(file)
    try {
      val f = Try {
        val bs = reader.readAllContent()
        val decompressed = CompressedStream.decompress(bs)
        Json.mapper.readValue[T](decompressed)
      }
      f match {
        // in this case we want to throw up as high as possible
        // this is not a case where we want to delete data because it is invalid
        case Failure(p@PasswordWrongException(_, _)) =>
          throw p;
        case _ =>
      }
      f
    } finally {
      reader.close()
    }
  }

  def writeToJson[T](file: File, value: T) = {
    file.getParentFile.mkdirs()
    val writer = config.newWriter(file)
    val bytes = Json.mapper.writer(new DefaultPrettyPrinter()).writeValueAsBytes(value)
    val compressed: BytesWrapper = CompressedStream.compress(BytesWrapper(bytes), CompressionMode.gzip)
    writer.write(compressed)
    writer.finish()
    journalHandler.addFileToJournal(file, writer.md5Hash())
  }
}

trait LifeCycle {
  def startup(): Future[Boolean]

  def finish(): Future[Boolean]
}