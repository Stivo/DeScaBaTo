package ch.descabato.core.actors

import java.io.File

import ch.descabato.core.LifeCycle
import ch.descabato.core.model.CompressedBlock
import ch.descabato.core.util.FileReader
import ch.descabato.core.util.FileWriter
import ch.descabato.utils.BytesWrapper
import ch.descabato.utils.Hash
import org.slf4j.LoggerFactory

import scala.concurrent.Future

class VolumeWriteActor(val context: BackupContext, val file: File) extends VolumeWriter {
  private val logger = LoggerFactory.getLogger(getClass)

  val filename = context.config.folder.toPath.relativize(file.toPath).toString

  private var _writer: FileWriter = _
  private var closed: Boolean = false

  private def writer = {
    if (_writer == null) {
      _writer = context.config.newWriter(file)
    }
    _writer
  }

  override def saveBlock(block: CompressedBlock): FilePosition = {
    require(!closed)
    val posBefore = writer.write(block.compressed)
    FilePosition(posBefore, block.compressed.length)
  }

  override def finish(): Future[Boolean] = {
    if (_writer != null && !closed) {
      writer.close()
      closed = true
    }
    Future.successful(true)
  }

  override def md5Hash: Future[Hash] = Future.successful(writer.md5Hash())

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

  override def read(filePosition: FilePosition): BytesWrapper = {
    reader.readChunk(filePosition.offset, filePosition.length)
  }

}

case class FilePosition(offset: Long, length: Long)

trait VolumeWriter extends LifeCycle {

  def md5Hash: Future[Hash]

  def saveBlock(block: CompressedBlock): FilePosition

  def currentPosition(): Long

  def filename: String
}

trait VolumeReader extends LifeCycle {
  def read(filePosition: FilePosition): BytesWrapper
}