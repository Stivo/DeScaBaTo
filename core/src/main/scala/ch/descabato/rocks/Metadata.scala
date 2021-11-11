package ch.descabato.rocks

import better.files._
import ch.descabato.CompressionMode
import ch.descabato.CustomByteArrayOutputStream
import ch.descabato.core.util.StandardNumberedFileType
import ch.descabato.rocks.protobuf.keys.FileMetadataKey
import ch.descabato.rocks.protobuf.keys.FileMetadataValue
import ch.descabato.rocks.protobuf.keys.RevisionValue
import ch.descabato.rocks.protobuf.keys.Status.FINISHED
import ch.descabato.rocks.protobuf.keys.ValueLogIndex
import ch.descabato.rocks.protobuf.keys.ValueLogStatusValue
import ch.descabato.utils.CompressedBytes
import ch.descabato.utils.CompressedStream
import ch.descabato.utils.FileUtils
import ch.descabato.utils.Implicits._
import ch.descabato.utils.Utils

import java.awt.Desktop
import java.awt.Desktop.Action
import java.io
import java.util.concurrent.Executors
import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

class RepairLogic(backupEnvInit: BackupEnvInit) extends Utils {

  private val fileManager = backupEnvInit.fileManager
  private val dbExportType = fileManager.dbexport
  private val volumeType = fileManager.volume

  private def getFiles(fileType: StandardNumberedFileType): (Seq[io.File], Seq[io.File]) = {
    fileType.getFiles().partition(x => !fileType.isTempFile(x))
  }

  private val (dbExportFiles, dbExportTempFiles) = getFiles(dbExportType)
  private val (volumeFiles, volumeTempFiles) = getFiles(volumeType)

  def deleteFile(x: io.File): Unit = {
    if (Desktop.isDesktopSupported && Desktop.getDesktop.isSupported(Action.MOVE_TO_TRASH)) {
      Desktop.getDesktop.moveToTrash(x)
    } else {
      x.delete()
    }
  }

  def deleteUnmentionedVolumes(toSeq: Seq[(ValueLogStatusKey, ValueLogStatusValue)]): Unit = {
    var volumes: Set[io.File] = volumeFiles.toSet
    var toDelete = Set.empty[io.File]
    for ((key, path) <- toSeq) {
      val file = backupEnvInit.config.resolveRelativePath(key.name)
      if (path.status == FINISHED) {
        logger.info(s"$key is marked as finished, will keep it")
        volumes -= file
      } else {
        toDelete += file
        logger.warn(s"Deleting $key, it is not marked as finished in dbexport")
      }
    }
    for (volume <- volumes) {
      logger.warn(s"Will delete $volume, because it is not mentioned in dbexport")
      deleteFile(volume)
    }
    for (volume <- toDelete) {
      logger.warn(s"Will delete $volume, because status is not finished")
      deleteFile(volume)
    }
  }

  def initialize(): KeyValueStore = {
    if (!backupEnvInit.readOnly) {
      deleteTempFiles()
    }
    val inMemoryDb = new DbMemoryImporter(backupEnvInit).importMetadata()
    if (!backupEnvInit.readOnly) {
      deleteUnmentionedVolumes(inMemoryDb.valueLogStatus.toSeq)
    }
    new KeyValueStore(backupEnvInit.readOnly, inMemoryDb)
  }

  def deleteTempFiles(): Unit = {
    for (file <- dbExportTempFiles) {
      logger.info(s"Deleting temp file $file")
      file.delete()
    }
    for (file <- volumeTempFiles) {
      logger.info(s"Deleting temp file $file")
      file.delete()
    }
  }

}

class DbExporter(rocksEnv: BackupEnv) extends Utils {
  val kvStore: KeyValueStore = rocksEnv.rocks
  private val filetype: StandardNumberedFileType = rocksEnv.fileManager.dbexport

  def exportUpdates(contentValues: Seq[ExportedEntry[_, _]]): Unit = {
    val backupTime = new StandardMeasureTime()

    val baos = new CustomByteArrayOutputStream()
    baos.write(CompressionMode.zstd9.getByte)
    val out = CompressedStream.getCompressor(CompressionMode.zstd9.getByte, baos)
    var totalLength = 0
    contentValues.foreach { e =>
      val wrapper = e.asValue()
      out.write(wrapper)
      totalLength += wrapper.length
    }
    out.close()

    for (valueLog <- new ValueLogWriter(rocksEnv, filetype, write = true, Long.MaxValue).autoClosed) {
      valueLog.write(new CompressedBytes(baos.toBytesWrapper, totalLength))
    }
    logger.info(s"Finished exporting updates, took " + backupTime.measuredTime())
  }
}


class DbMemoryImporter(backupEnvInit: BackupEnvInit) extends Utils {

  private val filetype: StandardNumberedFileType = backupEnvInit.fileManager.dbexport

  def importMetadata(): InMemoryDb = {
    val inMemoryDb: InMemoryDb = new InMemoryDb()
    val restoreTime = new StandardMeasureTime()

    // TODO could potentially be optimized in the future
    // decouple reading, processing from writing into inMemoryDb
    for (file <- filetype.getFiles()) {
      for {
        reader <- backupEnvInit.config.newReader(file).autoClosed
        decompressed <- CompressedStream.decompressToBytes(reader.readAllContent()).asInputStream().autoClosed
        encodedValueOption <- LazyList.continually(RevisionContentValue.readNextEntry(decompressed)).takeWhile(_.isDefined)
        encodedValue <- encodedValueOption
      } {
        val (deletion, key, value) = ColumnFamilies.decodeRevisionContentValue(encodedValue)
        if (deletion) {
          inMemoryDb.delete(key)
        } else {
          inMemoryDb.write(key, value.get)
        }
      }
    }

    logger.info(s"Finished importing metadata, took " + restoreTime.measuredTime())
    inMemoryDb
  }
}

class InMemoryDb {

  private var _fileMetadataKeyRepository = Map.empty[FileMetadataKey, FileMetadataKey]

  private var _chunks = Map.empty[ChunkKey, ValueLogIndex]
  private var _fileMetadata = Map.empty[FileMetadataKey, FileMetadataValue]
  private var _valueLogStatus = Map.empty[ValueLogStatusKey, ValueLogStatusValue]
  private var _revision = Map.empty[Revision, RevisionValue]

  def chunks: Map[ChunkKey, ValueLogIndex] = _chunks

  def fileMetadata: Map[FileMetadataKey, FileMetadataValue] = _fileMetadata

  def valueLogStatus: Map[ValueLogStatusKey, ValueLogStatusValue] = _valueLogStatus

  def revision: Map[Revision, RevisionValue] = _revision

  def readChunk(chunkKey: ChunkKey): Option[ValueLogIndex] = _chunks.get(chunkKey)

  def readFileMetadata(fileMetadata: FileMetadataKeyWrapper): Option[FileMetadataValue] = _fileMetadata.get(fileMetadata.fileMetadataKey)

  def readRevision(revision: Revision): Option[RevisionValue] = _revision.get(revision)

  def readValueLogStatus(valueLogStatusKey: ValueLogStatusKey): Option[ValueLogStatusValue] = _valueLogStatus.get(valueLogStatusKey)

  private def deduplicateFileMetadataKey(fileMetadataKey: FileMetadataKey): FileMetadataKey = {
    if (!_fileMetadataKeyRepository.contains(fileMetadataKey)) {
      _fileMetadataKeyRepository += fileMetadataKey -> fileMetadataKey
    }
    _fileMetadataKeyRepository(fileMetadataKey)
  }

  def delete(key: Key): Unit = {
    key match {
      case c@ChunkKey(_) =>
        _chunks -= c
      case f@FileMetadataKeyWrapper(_) =>
        _fileMetadata -= f.fileMetadataKey
      case s@ValueLogStatusKey(_) =>
        _valueLogStatus -= s
      case r@Revision(_) =>
        _revision -= r
      case x =>
        println(x)
        ???
    }
  }

  def exists(key: Key): Boolean = {
    key match {
      case c@ChunkKey(_) =>
        _chunks.contains(c)
      case f@FileMetadataKeyWrapper(_) =>
        _fileMetadata.contains(f.fileMetadataKey)
      case s@ValueLogStatusKey(_) =>
        _valueLogStatus.contains(s)
      case r@Revision(_) =>
        _revision.contains(r)
      case x =>
        println(x)
        ???
    }
  }

  def write[K <: Key, V](key: K, value: V): Unit = {
    key match {
      case c@ChunkKey(_) =>
        _chunks += c -> value.asInstanceOf[ValueLogIndex]
      case f@FileMetadataKeyWrapper(_) =>
        _fileMetadata += deduplicateFileMetadataKey(f.fileMetadataKey) -> value.asInstanceOf[FileMetadataValue]
      case s@ValueLogStatusKey(_) =>
        _valueLogStatus += s -> value.asInstanceOf[ValueLogStatusValue]
      case r@Revision(_) =>
        val value1 = value.asInstanceOf[RevisionValue]
        val copy = value1.copy(files = value1.files.map(deduplicateFileMetadataKey))
        _revision += r -> copy
      case x =>
        println(x)
        ???
    }

  }

}

