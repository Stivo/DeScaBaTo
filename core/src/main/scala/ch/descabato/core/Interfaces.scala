package ch.descabato.core

import java.io.File

import ch.descabato.CompressionMode
import ch.descabato.core.config.BackupFolderConfiguration
import ch.descabato.core.model._
import ch.descabato.core.util.Json
import ch.descabato.utils.BytesWrapper
import ch.descabato.utils.CompressedStream
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter

import scala.util.Failure
import scala.util.Try

trait ChunkIdResult

case class ChunkFound(id: Long) extends ChunkIdResult

case class ChunkIdAssigned(id: Long) extends ChunkIdResult

case object ChunkUnknown extends ChunkIdResult

sealed trait FileAlreadyBackedupResult

case object FileNotYetBackedUp extends FileAlreadyBackedupResult

case class FileAlreadyBackedUp(fileMetadata: FileMetadataStored) extends FileAlreadyBackedupResult

case object Storing extends FileAlreadyBackedupResult

trait JsonUser {
  def config: BackupFolderConfiguration

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
    writer.close()
  }
}
