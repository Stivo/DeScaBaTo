package ch.descabato.core

import java.io.File
import java.util.concurrent.CompletableFuture

import akka.actor.TypedActor
import akka.util.ByteString
import ch.descabato.CompressionMode
import ch.descabato.core.model.{Block, FileDescription, FileMetadata, StoredChunk}
import ch.descabato.core.util.Json
import ch.descabato.core_old.BackupFolderConfiguration
import ch.descabato.utils.{BytesWrapper, CompressedStream, Hash}
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter

import scala.compat.java8.FutureConverters
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

trait BlockStorage extends LifeCycle {
  def read(hash: Hash): Future[BytesWrapper]

  def hasAlready(block: Block): Future[Boolean]
  def hasAlreadyJava(block: Block): CompletableFuture[Block] = {
    val fut = new CompletableFuture[Block]()
    hasAlready(block).map { bool =>
      block.isAlreadySaved = bool
      fut.complete(block)
    }
    fut
  }

  def save(storedChunk: StoredChunk): Future[Boolean]

  def saveJava(storedChunk: StoredChunk): CompletableFuture[Boolean] = {
    FutureConverters.toJava(save(storedChunk)).toCompletableFuture
  }


}

trait BackupFileHandler extends LifeCycle with TypedActor.PreRestart {
  def backedUpFiles(): Future[Seq[FileMetadata]]

  def hasAlready(fileDescription: FileDescription): Future[Boolean]

  def saveFile(fileMetadata: FileMetadata): Future[Boolean]

  def saveFileJava(fileMetadata: FileMetadata): CompletableFuture[java.lang.Boolean] = {
    FutureConverters.toJava(saveFile(fileMetadata).map(_.asInstanceOf[java.lang.Boolean])).toCompletableFuture
  }

  def saveFileSameAsBefore(fd: FileDescription): Future[Boolean]
}

trait JsonUser {
  def config: BackupFolderConfiguration

  def readJson[T: Manifest](file: File): T = {
    val reader = config.newReader(file)
    val bs = reader.readAllContent()
    val decompressed = CompressedStream.decompress(bs)
    Json.mapper.readValue[T](decompressed)
  }

  def writeToJson[T](file: File, value: T) = {
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