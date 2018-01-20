package ch.descabato.core.actors

import java.io.File

import ch.descabato.core.LifeCycle
import ch.descabato.core.model.{Block, Length, StoredChunk}
import ch.descabato.core.util.{FileReader, FileWriter}
import ch.descabato.core_old.BackupFolderConfiguration
import ch.descabato.utils.BytesWrapper
import org.slf4j.LoggerFactory

import scala.concurrent.Future

class ChunkStorageActor(val config: BackupFolderConfiguration) extends ChunkHandler {
  val logger = LoggerFactory.getLogger(getClass)

  private val currentFileName = "blocks.kvs"
  val destinationFile = new File(config.folder, currentFileName)

  var _writer: FileWriter = _

  private def writer = {
    if (_writer == null) {
      _writer = config.newWriter(destinationFile)
    }
    _writer
  }

  var _reader: FileReader = _

  private def reader = {
    if (_reader == null) {
      _reader = config.newReader(destinationFile)
    }
    _reader
  }

  override def saveBlock(block: Block): Future[StoredChunk] = {
    val posBefore = writer.write(block.compressed)
    Future.successful(StoredChunk(currentFileName, block.hash, posBefore, Length(block.compressed.length)))
  }

  override def finish(): Future[Boolean] = {
    if (_writer != null) {
      writer.finish()
    }
    if (_reader != null) {
      reader.close()
    }
    Future.successful(true)
  }

  override def startup(): Future[Boolean] = {
    // nothing to do
    Future.successful(true)
  }

  override def read(storedChunk: StoredChunk): Future[BytesWrapper] = {
    Future.successful(reader.readChunk(storedChunk.startPos, storedChunk.length.size))
  }

}


trait ChunkHandler extends LifeCycle {
  def saveBlock(block: Block): Future[StoredChunk]

  def read(storedChunk: StoredChunk): Future[BytesWrapper]
}