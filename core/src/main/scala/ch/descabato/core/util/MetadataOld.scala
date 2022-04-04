package ch.descabato.core.util

import better.files._
import ch.descabato.CompressionMode
import ch.descabato.CustomByteArrayOutputStream
import ch.descabato.core.model.BackupEnv
import ch.descabato.core.model.BackupEnvInit
import ch.descabato.core.model.ChunkKey
import ch.descabato.core.model.ColumnFamilies
import ch.descabato.core.model.ExportedEntry
import ch.descabato.core.model.FileMetadataKeyWrapper
import ch.descabato.core.model.Key
import ch.descabato.core.model.KeyValueStore
import ch.descabato.core.model.Revision
import ch.descabato.core.model.RevisionContentValue
import ch.descabato.core.model.ValueLogStatusKey
import ch.descabato.protobuf.keys.FileMetadataKey
import ch.descabato.protobuf.keys.FileMetadataValue
import ch.descabato.protobuf.keys.RevisionValue
import ch.descabato.protobuf.keys.Status.FINISHED
import ch.descabato.protobuf.keys.ValueLogIndex
import ch.descabato.protobuf.keys.ValueLogStatusValue
import ch.descabato.utils.CompressedBytes
import ch.descabato.utils.CompressedStream
import ch.descabato.utils.Implicits._
import ch.descabato.utils.StandardMeasureTime
import ch.descabato.utils.Utils

class DbExporterOld(backupEnv: BackupEnv) extends Utils {
  val kvStore: KeyValueStore = backupEnv.rocks
  private val filetype: StandardNumberedFileType = backupEnv.fileManager.dbexport

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

    for (valueLog <- new ValueLogWriter(backupEnv, filetype, write = true, Long.MaxValue).autoClosed) {
      valueLog.write(new CompressedBytes(baos.toBytesWrapper, totalLength))
    }
    logger.info(s"Finished exporting updates, took " + backupTime.measuredTime())
  }
}


class DbMemoryImporterOld(backupEnvInit: BackupEnvInit) extends Utils {

  private val filetype: StandardNumberedFileType = backupEnvInit.fileManager.dbexport

  def importMetadata(): InMemoryDbOld = {
    val inMemoryDb: InMemoryDbOld = new InMemoryDbOld()
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

class InMemoryDbOld {

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

