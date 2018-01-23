package ch.descabato.core.actors

import java.io.File

import ch.descabato.core.LifeCycle
import ch.descabato.core.model.{Block, Length, StoredChunk}
import ch.descabato.core.util.{FileReader, FileWriter}
import ch.descabato.utils.BytesWrapper
import org.slf4j.LoggerFactory

import scala.concurrent.Future

class VolumeWriteActor(val context: BackupContext, val file: File) extends VolumeWriter {
  private val logger = LoggerFactory.getLogger(getClass)

  val filename = context.config.folder.toPath.relativize(file.toPath).toString

  private var _writer: FileWriter = _
  private var finished: Boolean = false

  private def writer = {
    if (_writer == null) {
      _writer = context.config.newWriter(file)
    }
    _writer
  }

  override def saveBlock(block: Block): StoredChunk = {
    require(!finished)
    val posBefore = writer.write(block.compressed)
    StoredChunk(filename, block.hash, posBefore, Length(block.compressed.length))
  }

  override def finish(): Boolean = {
    if (_writer != null && !finished) {
      writer.finish()
      finished = true
    }
    true
  }

  override def startup(): Future[Boolean] = {
    // nothing to do
    Future.successful(true)
  }

  override def currentPosition(): Long = {
    writer.currentPosition()
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

  override def read(storedChunk: StoredChunk): BytesWrapper = {
    reader.readChunk(storedChunk.startPos, storedChunk.length.size)
  }

}

trait VolumeWriter extends LifeCycle {
  def saveBlock(block: Block): StoredChunk

  def currentPosition(): Long

  def filename(): String
}

trait VolumeReader extends LifeCycle {
  def read(storedChunk: StoredChunk): BytesWrapper
}