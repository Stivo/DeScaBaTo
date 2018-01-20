package ch.descabato.core.actors

import java.io.File

import ch.descabato.core.LifeCycle
import ch.descabato.core.model.{Block, Length, StoredChunk}
import ch.descabato.core.util.{FileReader, FileWriter}
import ch.descabato.utils.BytesWrapper
import ch.descabato.utils.Implicits._
import org.slf4j.LoggerFactory

import scala.concurrent.Future

class ChunkStorageActor(val context: BackupContext) extends ChunkHandler {
  val logger = LoggerFactory.getLogger(getClass)

  private val currentFile = context.fileManager.volume.nextFile()
  private val currentFileName = context.config.folder.toPath.relativize(currentFile.toPath).toString

  var _writer: FileWriter = _

  private def writer = {
    if (_writer == null) {
      _writer = context.config.newWriter(currentFile)
    }
    _writer
  }

  var _readers: Map[String, FileReader] = Map.empty

  private def reader(name: String) = {
    if (!_readers.safeContains(name)) {
      val reader = context.config.newReader(new File(context.config.folder, name))
      _readers += name -> reader
    }
    _readers(name)
  }

  override def saveBlock(block: Block): Future[StoredChunk] = {
    val posBefore = writer.write(block.compressed)
    Future.successful(StoredChunk(currentFileName, block.hash, posBefore, Length(block.compressed.length)))
  }

  override def finish(): Future[Boolean] = {
    if (_writer != null) {
      writer.finish()
    }
    for (reader <- _readers.values) {
      reader.close()
    }
    Future.successful(true)
  }

  override def startup(): Future[Boolean] = {
    // nothing to do
    Future.successful(true)
  }

  override def read(storedChunk: StoredChunk): Future[BytesWrapper] = {
    Future.successful(reader(storedChunk.file).readChunk(storedChunk.startPos, storedChunk.length.size))
  }

}


trait ChunkHandler extends LifeCycle {
  def saveBlock(block: Block): Future[StoredChunk]

  def read(storedChunk: StoredChunk): Future[BytesWrapper]
}