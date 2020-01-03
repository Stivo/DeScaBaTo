package ch.descabato.rocks

import java.io
import java.nio.charset.StandardCharsets
import java.util

import better.files._
import ch.descabato.CompressionMode
import ch.descabato.CustomByteArrayOutputStream
import ch.descabato.core.JsonUser
import ch.descabato.core.actors.BlockingOperation
import ch.descabato.core.actors.JournalHandler
import ch.descabato.core.actors.MetadataStorageActor.BackupMetaDataStored
import ch.descabato.core.config.BackupFolderConfiguration
import ch.descabato.core.model.BackupDescriptionStored
import ch.descabato.core.model.FileAttributes
import ch.descabato.core.model.StoredChunk
import ch.descabato.core.util.FileManager
import ch.descabato.core.util.FileType
import ch.descabato.core.util.StandardNumberedFileType
import ch.descabato.rocks.protobuf.keys.BackedupFileType
import ch.descabato.rocks.protobuf.keys.FileMetadataKey
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

import scala.collection.mutable

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

  def readStatus(rocks: RocksDbKeyValueStore): Option[RocksStates.Value] = {
    RocksStates.fromBytes(rocks.readDefault(RepairLogic.rocksStatusKey))
  }

  def setStateTo(rocksStates: RocksStates.Value, rocksDbKeyValueStore: RocksDbKeyValueStore): Unit = {
    rocksDbKeyValueStore.writeDefault(rocksStatusKey, rocksStates.asBytes())
  }
}

class RepairLogic(rocksEnvInit: RocksEnvInit) extends Utils {

  import RocksStates._

  private val initialRocks: RocksDbKeyValueStore = RocksDbKeyValueStore(rocksEnvInit)
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

  def initialize(): RocksDbKeyValueStore = {
    if (isConsistent()) {
      logger.info("Backup loaded without issues")
      // return, all is fine
      initialRocks
    } else {
      if (rocksEnvInit.readOnly) {
        // return as is, tell user to repair
        // maybe list problems
        throw new IllegalArgumentException("Database is in an inconsistent state and no repair allowed. Please call repair.")
      } else {
        // repair
        if (rocksEnvInit.startedWithoutRocksdb || initialRockState == Some(Reconstructing)) {
          logger.warn("Need to reconstruct rocksdb from metadata. Rocks folder seems to have been lost since last usage.")
          closeRocks()
          deleteRocksFolder()
          deleteTempFiles()
          // propose compaction
          reimport()
        } else {
          logger.warn("Need to cleanup temp files.")
          eitherDeleteTempFilesOrRename(initialRocks)
          // propose compaction
          RepairLogic.setStateTo(Consistent, initialRocks)
          initialRocks.commit()
          initialRocks
        }
      }
    }
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

  def eitherDeleteTempFilesOrRename(rocks: RocksDbKeyValueStore): Unit = {

    def eitherDeleteOrRename(tempFiles: Seq[io.File], fileType: StandardNumberedFileType): Unit = {
      for (tempFile <- tempFiles) {
        val relativePath = rocksEnvInit.config.relativePath(fileType.finalNameForTempFile(tempFile))
        val key = ValueLogStatusKey(relativePath)
        val status = rocks.readValueLogStatus(key)
        status match {
          case Some(ValueLogStatusValue(Status.FINISHED, _, _)) =>
            logger.info(s"File $tempFile is marked as finished in rocksdb, so renaming to final name.")
            fileType.renameTempFileToFinal(tempFile)
          case Some(ValueLogStatusValue(Status.WRITING, _, _)) =>
            logger.info(s"File $tempFile is marked as writing in rocksdb, so deleting it.")
            tempFile.delete()
            val newStatus = status.map { s =>
              s.copy(status = Status.DELETED, size = -1L, ByteString.EMPTY)
            }.get
            rocks.write(key, newStatus)
          case None =>
            // this should not be possible
            ???
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

  def reimport(): RocksDbKeyValueStore = {
    if (rocksEnvInit.readOnly) {
      throw new IllegalArgumentException("May not be called when database is opened read only")
    }
    val rocks = RocksDbKeyValueStore(rocksEnvInit)
    RepairLogic.setStateTo(RocksStates.Reconstructing, rocks)
    rocks.commit()
    new DbImporter(rocksEnvInit, rocks).importMetadata()
    RepairLogic.setStateTo(RocksStates.Consistent, rocks)
    rocks.commit()
    rocks
  }

}

class DbExporter(rocksEnv: RocksEnv) extends Utils {
  val kvStore: RocksDbKeyValueStore = rocksEnv.rocks
  private val filetype: StandardNumberedFileType = rocksEnv.fileManager.dbexport

  def exportUpdates(): Unit = {
    val backupTime = new StandardMeasureTime()

    val contentValues = kvStore.readAllUpdates()

    val baos = new CustomByteArrayOutputStream()
    contentValues.foreach { case (_, value) =>
      baos.write(value.asArray())
    }
    for (valueLog <- new ValueLogWriter(rocksEnv, filetype, write = true, rocksEnv.config.volumeSize.bytes).autoClosed) {
      valueLog.write(CompressedStream.compressBytes(baos.toBytesWrapper, CompressionMode.gzip))
    }
    contentValues.foreach { case (key, _) =>
      kvStore.delete(key, writeAsUpdate = false)
    }
    logger.info(s"Finished exporting updates, took " + backupTime.measuredTime())
    RepairLogic.setStateTo(RocksStates.Consistent, kvStore)
    kvStore.commit()
  }
}


class DbImporter(rocksEnvInit: RocksEnvInit, kvStore: RocksDbKeyValueStore) extends Utils {

  private val filetype: StandardNumberedFileType = rocksEnvInit.fileManager.dbexport

  def importMetadata(): Unit = {
    val restoreTime = new StandardMeasureTime()

    for (file <- filetype.getFiles()) {
      for {
        reader <- rocksEnvInit.config.newReader(file).autoClosed
        decompressed <- CompressedStream.decompressToBytes(reader.readAllContent()).asInputStream().autoClosed
        encodedValueOption <- LazyList.continually(RevisionContentValue.readNextEntry(decompressed)).takeWhile(_.isDefined)
        encodedValue <- encodedValueOption
      } {
        val (deletion, key, value) = kvStore.decodeRevisionContentValue(encodedValue)
        if (deletion) {
          kvStore.delete(key, writeAsUpdate = false)
        } else {
          kvStore.write(key, value.get, writeAsUpdate = false)
        }
      }
      kvStore.commit()
    }
    for (file <- filetype.getFiles()) {
      val relativePath = rocksEnvInit.config.relativePath(file)
      val key = ValueLogStatusKey(relativePath)
      val status = kvStore.readValueLogStatus(key)
      if (status.isEmpty) {
        logger.info(s"Adding status = finished for $relativePath")
        kvStore.write(key, ValueLogStatusValue(Status.FINISHED, file.length(), ByteString.copyFrom(file.toScala.digest("MD5"))))
      }
    }
    logger.info(s"Finished importing metadata, took " + restoreTime.measuredTime())
    kvStore.commit()
  }
}

class OldDataImporter(rocksEnv: RocksEnv, val overrideReadFromConfig: Option[BackupFolderConfiguration] = None) extends Utils with JsonUser {

  import rocksEnv._

  val fm = overrideReadFromConfig.map(new FileManager(_)).getOrElse(rocksEnvInit.fileManager)

  def importMetadata(): Unit = {
    val restoreTime = new StandardMeasureTime()

    RepairLogic.setStateTo(RocksStates.Reconstructing, rocks)
    val chunksById = mutable.Map.empty[Long, Array[Byte]]
    val filesById = mutable.Map.empty[Long, FileMetadataKey]
    importChunks(chunksById)
    importFilesAndFolders(chunksById, filesById)
    importRevisions(filesById)
    markVolumesAsDone()
    logger.info(s"Finished importing chunks, took " + restoreTime.measuredTime())
    RepairLogic.setStateTo(RocksStates.Consistent, rocks)
    rocks.commit()
    if (overrideReadFromConfig.isEmpty) {
      renameFolder(fm.volumeIndex)
      renameFolder(fm.metadata)
      renameFolder(fm.backup)
    }
  }

  private def renameFolder(fileType: FileType): Unit = {

    fileType.getFiles().foreach { file =>
      file.renameTo(new io.File(file.getParentFile, "old_" + file.getName))
    }
  }

  private def importChunks(chunksById: mutable.Map[Long, Array[Byte]]): Unit = {
    for (file <- fm.volumeIndex.getFiles()) {
      val json = readJson[Seq[StoredChunk]](file)
      for (index <- json.get) {
        val file1 = index.file
        chunksById += index.id -> index.hash
        rocks.write(ChunkKey(index.hash), ValueLogIndex(filename = file1, from = index.startPos, lengthCompressed = index.length.toInt, lengthUncompressed = -1))
      }
      rocks.commit()
    }
  }

  def patchPath(path: String): String = {
    path
    // just needed for one import path.replaceAllLiterally("D:", "E:")
  }

  private def importFilesAndFolders(chunksById: mutable.Map[Long, Array[Byte]], filesById: mutable.Map[Long, FileMetadataKey]): Unit = {
    for (file <- fm.metadata.getFiles()) {
      val json = readJson[BackupMetaDataStored](file).get
      for (file <- json.files) {
        val time = file.fd.attrs.lastModifiedTime match {
          case l: Number => l.longValue()
          case null => -1L
          case _ => ???
        }
        val keyWrapper = FileMetadataKeyWrapper(FileMetadataKey(BackedupFileType.FILE, path = patchPath(file.fd.path), time))
        val hashes = file.chunkIds.map(id =>
          chunksById(id)
        ).fold(Array.empty)(_ ++ _)
        val value = createMetadataValue(BackedupFileType.FILE, file.fd.attrs, Some(hashes), file.fd.size)
        rocks.write(keyWrapper, value)
        filesById += file.id -> keyWrapper.fileMetadataKey
      }
      for (folder <- json.folders) {
        val time = folder.folderDescription.attrs.lastModifiedTime match {
          case l: Number => l.longValue()
          case null => -1L
          case _ => ???
        }
        val keyWrapper = FileMetadataKeyWrapper(FileMetadataKey(BackedupFileType.FOLDER, path = patchPath(folder.folderDescription.path), time))
        val value = createMetadataValue(BackedupFileType.FOLDER, folder.folderDescription.attrs, None, -1L)
        rocks.write(keyWrapper, value)
        filesById += folder.id -> keyWrapper.fileMetadataKey
      }
      for (symLink <- json.symlinks) {
        logger.info(s"Can not import symlink $symLink")
      }
      rocks.commit()
    }
  }

  def createMetadataValue(filetype: BackedupFileType, attrs: FileAttributes, hashes: Option[Array[Byte]], length: Long) = {
    val creationTime = attrs.get(FileAttributes.creationTime) match {
      case l: Number => l.longValue()
      case null => 0L
      case _ => ???
    }
    val dosReadOnly = attrs.get("dos:readonly") match {
      case x: Boolean => x
      case _ => false
    }
    val dosHidden = attrs.get("dos:hidden") match {
      case x: Boolean => x
      case _ => false
    }
    FileMetadataValue(
      filetype = filetype,
      hashes = hashes.map(ByteString.copyFrom).getOrElse(ByteString.EMPTY),
      length = length,
      created = creationTime,
      dosIsReadonly = dosReadOnly,
      dosIsHidden = dosHidden,
    )
  }

  def importRevisions(filesById: mutable.Map[Long, FileMetadataKey]): Unit = {
    for ((file, index) <- fm.backup.getFiles().sortBy(fm.backup.dateOfFile).zipWithIndex) {
      val revision = readJson[BackupDescriptionStored](file).get
      val key = Revision(index)
      val date = fm.backup.dateOfFile(file).toInstant.toEpochMilli
      val allIds = revision.fileIds ++ revision.dirIds
      val files = allIds.flatMap(x => filesById.get(x)).sortBy(_.path)
      val value = RevisionValue(created = date, files = files.toSeq)
      rocks.write(key, value)
    }
    rocks.commit()
  }

  def markVolumesAsDone(): Unit = {
    for (file <- fm.volume.getFiles()) {
      rocks.write(ValueLogStatusKey(config.relativePath(file)), ValueLogStatusValue(Status.FINISHED, size = file.length()))
    }
    rocks.commit()
  }

  override def config: BackupFolderConfiguration = overrideReadFromConfig.getOrElse(rocksEnvInit.config)

  override def writeToJson[T](file: io.File, value: T): BlockingOperation = {
    // should not be used to write
    ???
  }

  override def journalHandler: JournalHandler = {
    // implementation not needed for reading
    ???
  }
}