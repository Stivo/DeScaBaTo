package ch.descabato.core.actors

import java.io.File

import ch.descabato.core.LifeCycle
import ch.descabato.core.model.{Block, Length, StoredChunk}
import ch.descabato.core.util.{FileReader, FileWriter}
import ch.descabato.utils.BytesWrapper
import org.slf4j.LoggerFactory

import scala.concurrent.Future

class VolumeWriteActor(val context: BackupContext, val file: File) extends VolumeWriter {
  val logger = LoggerFactory.getLogger(getClass)

  private val filename = context.config.folder.toPath.relativize(file.toPath).toString

  var _writer: FileWriter = _

  private def writer = {
    if (_writer == null) {
      _writer = context.config.newWriter(file)
    }
    _writer
  }

  override def saveBlock(block: Block): Future[StoredChunk] = {
    val posBefore = writer.write(block.compressed)
    Future.successful(StoredChunk(filename, block.hash, posBefore, Length(block.compressed.length)))
  }

  override def finish(): Future[Boolean] = {
    if (_writer != null) {
      writer.finish()
    }
    Future.successful(true)
  }

  override def startup(): Future[Boolean] = {
    // nothing to do
    Future.successful(true)
  }

}

class VolumeReadActor(val context: BackupContext, val file: File) extends VolumeReader {
  val logger = LoggerFactory.getLogger(getClass)

  private val reader: FileReader = context.config.newReader(file)

  override def finish(): Future[Boolean] = {
    reader.close()
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

trait VolumeWriter extends LifeCycle {
  def saveBlock(block: Block): Future[StoredChunk]
}

trait VolumeReader extends LifeCycle {
  def read(storedChunk: StoredChunk): Future[BytesWrapper]
}