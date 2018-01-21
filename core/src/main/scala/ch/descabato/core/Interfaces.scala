package ch.descabato.core

import java.io.File
import java.util.Date

import akka.actor.TypedActor
import ch.descabato.CompressionMode
import ch.descabato.core.actors.MetadataActor.BackupMetaData
import ch.descabato.core.actors.MyEventReceiver
import ch.descabato.core.model.{Block, FileMetadata}
import ch.descabato.core.util.Json
import ch.descabato.core_old.{BackupFolderConfiguration, FileDescription, FolderDescription}
import ch.descabato.utils.{BytesWrapper, CompressedStream, Hash}
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter

import scala.concurrent.Future

trait BlockStorage extends LifeCycle {
  def read(hash: Hash): Future[BytesWrapper]

  def hasAlready(block: Block): Future[Boolean] = hasAlready(block.hash)

  def hasAlready(hash: Hash): Future[Boolean]

  def save(block: Block): Future[Boolean]
}

sealed trait FileAlreadyBackedupResult
case object FileNotYetBackedUp extends FileAlreadyBackedupResult
case class FileAlreadyBackedUp(fileMetadata: FileMetadata) extends FileAlreadyBackedupResult
case object Storing extends FileAlreadyBackedupResult

trait BackupFileHandler extends LifeCycle with TypedActor.PreRestart with MyEventReceiver {
  def retrieveBackup(date: Option[Date] = None): Future[BackupMetaData]

  def addDirectory(description: FolderDescription): Future[Boolean]

  def hasAlready(fileDescription: FileDescription): Future[FileAlreadyBackedupResult]

  def saveFile(fileMetadata: FileMetadata): Future[Boolean]

  def saveFileSameAsBefore(fd: FileDescription): Future[Boolean]
}

trait JsonUser {
  def config: BackupFolderConfiguration

  def readJson[T: Manifest](file: File): T = {
    val reader = config.newReader(file)
    val bs = reader.readAllContent()
    reader.close()
    //val decompressed = CompressedStream.decompress(bs)
    Json.mapper.readValue[T](bs.asArray())
  }

  def writeToJson[T](file: File, value: T) = {
    file.getParentFile.mkdirs()
    val writer = config.newWriter(file)
    val bytes = Json.mapper.writer(new DefaultPrettyPrinter()).writeValueAsBytes(value)
    val compressed: BytesWrapper = CompressedStream.compress(new BytesWrapper(bytes), CompressionMode.deflate)
    writer.write(new BytesWrapper(bytes))
    writer.finish()
  }
}

trait LifeCycle {
  def startup(): Future[Boolean]
  def finish(): Future[Boolean]
}