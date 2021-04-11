package ch.descabato.rocks

import java.io
import java.nio.charset.StandardCharsets
import java.util

import better.files._
import ch.descabato.CompressionMode
import ch.descabato.CustomByteArrayOutputStream
import ch.descabato.core.util.StandardNumberedFileType
import ch.descabato.rocks.protobuf.keys.FileMetadataValue
import ch.descabato.rocks.protobuf.keys.RevisionValue
import ch.descabato.rocks.protobuf.keys.Status
import ch.descabato.rocks.protobuf.keys.ValueLogIndex
import ch.descabato.rocks.protobuf.keys.ValueLogStatusValue
import ch.descabato.utils.CompressedStream
import ch.descabato.utils.FileUtils
import ch.descabato.utils.Implicits._
import ch.descabato.utils.Utils
import com.google.protobuf.ByteString

object RocksStates extends Enumeration {

  protected case class Val(name: String) extends super.Val {
    def asBytes(): Array[Byte] = {
      super.toString().getBytes(StandardCharsets.UTF_8)
    }
  }

  import scala.language.implicitConversions

  implicit def valueToRocksStateVal(x: Value): Val = x.asInstanceOf[Val]

  val Consistent: Val = Val("Consistent")
  val Writing: Val = Val("Writing")
  val Reconstructing: Val = Val("Reconstructing")

  def fromBytes(value: Array[Byte]): Option[Value] = {
    Option(value).flatMap(value => values.find(v => util.Arrays.equals(v.asBytes(), value)))
  }

}

object RepairLogic {

  val rocksStatusKey: Array[Byte] = "rocks_status".getBytes

  def readStatus(rocks: KeyValueStore): Option[RocksStates.Value] = {
    RocksStates.fromBytes(rocks.readDefault(RepairLogic.rocksStatusKey))
  }

  def setStateTo(rocksStates: RocksStates.Value, rocksDbKeyValueStore: KeyValueStore): Unit = {
    rocksDbKeyValueStore.writeDefault(rocksStatusKey, rocksStates.asBytes())
  }
}

class RepairLogic(rocksEnvInit: RocksEnvInit) extends Utils {

  import RocksStates._

  private val initialRocks: KeyValueStore = KeyValueStore(rocksEnvInit)
  private val initialRockState = RepairLogic.readStatus(initialRocks)

  private val fileManager = rocksEnvInit.fileManager
  private val dbExportType = fileManager.dbexport
  private val volumeType = fileManager.volume

  private def getFiles(fileType: StandardNumberedFileType): (Seq[io.File], Seq[io.File]) = {
    fileType.getFiles().partition(x => !fileType.isTempFile(x))
  }

  private val (dbExportFiles, dbExportTempFiles) = getFiles(dbExportType)
  private val allDbExportFiles = dbExportFiles ++ dbExportTempFiles
  private val (volumeFiles, volumeTempFiles) = getFiles(volumeType)
  private val allVolumeFiles = volumeFiles ++ volumeTempFiles


  def isConsistent(): Boolean = {
    // are there no temp files?
    val noTempFiles = volumeTempFiles.isEmpty && dbExportTempFiles.isEmpty
    // is the rocksdb available and consistent?
    val isConsistent = initialRockState == Some(Consistent)
    // Are both metadata and rocksdb missing? => New, that is also consistent
    val isNew = allDbExportFiles.isEmpty && allVolumeFiles.isEmpty && initialRockState.isEmpty
    noTempFiles && (isConsistent || isNew)
  }

  def initialize(ignoreIssues: Boolean): KeyValueStore = {
    ???
  }

  def deleteTempFiles(): Unit = {
    val fm = rocksEnvInit.fileManager
    for (file <- fm.dbexport.getFiles()) {
      if (fm.dbexport.isTempFile(file)) {
        logger.info(s"Deleting temp file $file")
        file.delete()
      }
    }
    for (file <- fm.volume.getFiles()) {
      if (fm.volume.isTempFile(file)) {
        logger.info(s"Deleting temp file $file")
        file.delete()
      }
    }
  }

  def eitherDeleteTempFilesOrRename(rocks: KeyValueStore): Unit = {

    def eitherDeleteOrRename(tempFiles: Seq[io.File], fileType: StandardNumberedFileType): Unit = {
      for (tempFile <- tempFiles) {
        val relativePath = rocksEnvInit.config.relativePath(fileType.finalNameForTempFile(tempFile))
        val key = ValueLogStatusKey(relativePath)
        val status = rocks.readValueLogStatus(key)
        status match {
          case Some(ValueLogStatusValue(Status.FINISHED, _, _)) =>
            logger.info(s"File $tempFile is marked as finished in rocksdb, so renaming to final name.")
            fileType.renameTempFileToFinal(tempFile)
          case Some(ValueLogStatusValue(s@(Status.WRITING | Status.DELETED | Status.MARKED_FOR_DELETION), _, _)) =>
            logger.info(s"File $tempFile is marked as $s in rocksdb, so deleting it.")
            tempFile.delete()
            val newStatus = status.map { s =>
              s.copy(status = Status.DELETED, size = -1L, ByteString.EMPTY)
            }.get
            rocks.write(key, newStatus)
          case None =>
            logger.info(s"File $tempFile is not mentioned in rocksdb, so just delete it.")
            tempFile.delete()
        }
      }
    }

    eitherDeleteOrRename(volumeTempFiles, volumeType)
    eitherDeleteOrRename(dbExportTempFiles, dbExportType)
    rocks.commit()
  }

  def closeRocks(): Unit = {
    initialRocks.close()
  }

  def deleteRocksFolder(): Unit = {
    FileUtils.deleteAll(rocksEnvInit.rocksFolder)
  }

}

class DbExporter(rocksEnv: RocksEnv) extends Utils {
  val kvStore: KeyValueStore = rocksEnv.rocks
  private val filetype: StandardNumberedFileType = rocksEnv.fileManager.dbexport

  def exportUpdates(): Unit = {
    val backupTime = new StandardMeasureTime()

    val contentValues = kvStore.readAllUpdates()

    val baos = new CustomByteArrayOutputStream()
    contentValues.foreach { case (_, value) =>
      baos.write(value.asArray())
    }
    for (valueLog <- new ValueLogWriter(rocksEnv, filetype, write = true, rocksEnv.config.volumeSize.bytes).autoClosed) {
      valueLog.write(CompressedStream.compressBytes(baos.toBytesWrapper, CompressionMode.zstd9))
    }
    contentValues.foreach { case (key, _) =>
      kvStore.delete(key)
    }
    logger.info(s"Finished exporting updates, took " + backupTime.measuredTime())
    RepairLogic.setStateTo(RocksStates.Consistent, kvStore)
    kvStore.commit()
  }
}


class DbMemoryImporter(rocksEnvInit: RocksEnvInit, kvStore: KeyValueStore) extends Utils {

  private val filetype: StandardNumberedFileType = rocksEnvInit.fileManager.dbexport

  def importMetadata(): InMemoryDb = {
    val inMemoryDb: InMemoryDb = new InMemoryDb()
    val restoreTime = new StandardMeasureTime()

    for (file <- filetype.getFiles()) {
      for {
        reader <- rocksEnvInit.config.newReader(file).autoClosed
        decompressed <- CompressedStream.decompressToBytes(reader.readAllContent()).asInputStream().autoClosed
        encodedValueOption <- LazyList.continually(RevisionContentValue.readNextEntry(decompressed)).takeWhile(_.isDefined)
        encodedValue <- encodedValueOption
      } {
        ???
        //        val (deletion, key, value) = kvStore.decodeRevisionContentValue(encodedValue)
        //        if (deletion) {
        //          inMemoryDb.delete(key)
        //        } else {
        //          inMemoryDb.write(key, value.get)
        //        }
      }
    }
    logger.info(s"Finished importing metadata, took " + restoreTime.measuredTime())
    inMemoryDb
  }
}

class InMemoryDb {

  private var _chunks = Map.empty[ChunkKey, ValueLogIndex]
  private var _fileMetadata = Map.empty[FileMetadataKeyWrapper, FileMetadataValue]
  private var _valueLogStatus = Map.empty[ValueLogStatusKey, ValueLogStatusValue]
  private var _revision = Map.empty[Revision, RevisionValue]

  @deprecated
  def chunks: Map[ChunkKey, ValueLogIndex] = _chunks

  @deprecated
  def fileMetadata: Map[FileMetadataKeyWrapper, FileMetadataValue] = _fileMetadata

  @deprecated
  def valueLogStatus: Map[ValueLogStatusKey, ValueLogStatusValue] = _valueLogStatus

  @deprecated
  def revision: Map[Revision, RevisionValue] = _revision

  def readChunk(chunkKey: ChunkKey): ValueLogIndex = _chunks(chunkKey)

  def readFileMetadata(fileMetadata: FileMetadataKeyWrapper): FileMetadataValue = _fileMetadata(fileMetadata)

  def readRevision(revision: Revision): RevisionValue = _revision(revision)

  def readValueLogStatus(valueLogStatusKey: ValueLogStatusKey): ValueLogStatusValue = _valueLogStatus(valueLogStatusKey)

  def delete(key: Key): Unit = {
    key match {
      case c@ChunkKey(_) =>
        _chunks -= c
      case f@FileMetadataKeyWrapper(_) =>
        _fileMetadata -= f
      case s@ValueLogStatusKey(_) =>
        _valueLogStatus -= s
      case r@Revision(_) =>
        _revision -= r
      case x =>
        println(x)
        ???
    }
  }

  def write[K <: Key, V](key: K, value: V): Unit = {
    // TODO add change tracking
    key match {
      case c@ChunkKey(_) =>
        _chunks += c -> value.asInstanceOf[ValueLogIndex]
      case f@FileMetadataKeyWrapper(_) =>
        _fileMetadata += f -> value.asInstanceOf[FileMetadataValue]
      case s@ValueLogStatusKey(_) =>
        _valueLogStatus += s -> value.asInstanceOf[ValueLogStatusValue]
      case r@Revision(_) =>
        _revision += r -> value.asInstanceOf[RevisionValue]
      case x =>
        println(x)
        ???
    }

  }

}

