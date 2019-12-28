package ch.descabato.rocks

import java.io.ByteArrayOutputStream
import java.io.File
import java.io.FileInputStream
import java.io.OutputStream
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.attribute.DosFileAttributes

import better.files._
import ch.descabato.CustomByteArrayOutputStream
import ch.descabato.core.IgnoreFileMatcher
import ch.descabato.core.config.BackupFolderConfiguration
import ch.descabato.core.util.StandardNumberedFileType
import ch.descabato.frontend.BackupConf
import ch.descabato.rocks.protobuf.keys.FileMetadataKey
import ch.descabato.rocks.protobuf.keys.FileMetadataValue
import ch.descabato.rocks.protobuf.keys.FileType
import ch.descabato.rocks.protobuf.keys.RevisionValue
import ch.descabato.rocks.protobuf.keys.Status
import ch.descabato.rocks.protobuf.keys.ValueLogStatusValue
import ch.descabato.utils.BytesWrapper
import ch.descabato.utils.CompressedStream
import ch.descabato.utils.Implicits._
import ch.descabato.utils.Streams.VariableBlockOutputStream
import ch.descabato.utils.Utils
import com.google.protobuf.ByteString
import com.typesafe.scalalogging.LazyLogging

import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.Try

object Backup {

  def listFiles(folder: File): Iterator[Path] = {
    Files.walk(folder.toPath).iterator().asScala
  }

}

class RunBackup(backupConf: BackupConf, config: BackupFolderConfiguration) extends AutoCloseable with LazyLogging {

  private val rocksEnv = RocksEnv(config, readOnly = false)

  def run(file: File): Unit = {
    val backupper = new Backupper(backupConf, rocksEnv)
    backupper.backup(file)
    backupper.printStatistics()
    //    val totalSize = listFiles(dbFolder).map(_.toFile.length()).sum
    //    val files = listFiles(dbFolder).size
    //    println(s"Created $files files with ${Utils.readableFileSize(totalSize)}")
  }

  override def close(): Unit = {
    rocksEnv.close()
  }
}

class Backupper(backupConf: BackupConf, rocksEnv: RocksEnv) extends LazyLogging {

  import rocksEnv._

  val ignoreFileMatcher = new IgnoreFileMatcher(rocksEnv.config.ignoreFile)

  def printStatistics(): Unit = {
    logger.info(s"Chunks found: ${chunkFoundCounter}")
    logger.info(s"Chunks not found: ${chunkNotFoundCounter}")
    logger.info(StopWatch.report)
  }

  var revisionContent: ByteArrayOutputStream = new ByteArrayOutputStream()
  var filesInRevision: mutable.Buffer[FileMetadataKey] = mutable.Buffer.empty

  val valueLog = new ValueLogWriter(rocksEnv, rocksEnv.fileManager.volume, write = true, backupConf.volumeSize().bytes)

  def findNextRevision(): Int = {
    val iterator = rocks.getAllRevisions()
    if (iterator.isEmpty) {
      1
    } else {
      iterator.map(_._1.number).max + 1
    }
  }

  def backup(str: File*): Unit = {
    val revision = Revision(findNextRevision())
    val mt = new StandardMeasureTime()

    for (folderToBackup <- str) {
      for (file <- Backup.listFiles(folderToBackup) if ignoreFileMatcher.pathIsNotIgnored(folderToBackup.toPath, file)) {
        backupFileOrFolderIfNecessary(file)
      }
    }
    logger.info("Took " + mt.measuredTime())
    val value = RevisionValue(System.currentTimeMillis(), filesInRevision.toSeq)
    rocks.write(revision, value)
    logger.info("Closing valuelog")
    valueLog.close()
    rocks.commit()
    new MetadataExporter(rocksEnv).exportUpdates()
    val compacting = new StandardMeasureTime()
    rocks.compact()
    logger.info("Compaction took " + compacting.measuredTime())
  }

  private val buffer = Array.ofDim[Byte](1024 * 1024)
  private val digest = rocksEnv.config.createMessageDigest()

  var chunkNotFoundCounter = 0
  var chunkFoundCounter = 0

  val hashTiming = new StopWatch("sha256")
  val buzhashTiming = new StopWatch("buzhash")

  def backupChunk(bytesWrapper: BytesWrapper, hashList: OutputStream): Unit = {
    //    println(s"Backing up chunk with length ${toByteArray.length}")
    val hash = hashTiming.measure {
      digest.reset()
      digest.digest(bytesWrapper)
    }
    val chunkKey = ChunkKey(hash)
    if (rocks.exists(chunkKey)) {
      chunkFoundCounter += 1
    } else {
      val (index, commit) = valueLog.write(bytesWrapper)
      rocks.write(chunkKey, index)
      if (commit) {
        rocks.commit()
      }
      chunkNotFoundCounter += 1
    }
    hashList.write(hash)
  }

  def backupFile(file: Path): FileMetadataValue = {
    val hashList = new CustomByteArrayOutputStream()
    logger.info(file.toFile.toString)
    var chunk = 0
    for {
      fis <- new FileInputStream(file.toFile).autoClosed
      chunker <- new VariableBlockOutputStream({ wrapper =>
        backupChunk(wrapper, hashList)
        logger.trace(s"Chunk $chunk with length ${wrapper.length}")
        chunk += 1
      }).autoClosed
    } {
      fis.pipeTo(chunker, buffer)
    }
    fillInTypeInfo(file, Some(hashList.toBytesWrapper))
  }

  private def backupFileOrFolderIfNecessary(file: Path): Unit = {
    val filetype = if (file.toFile.isFile) FileType.FILE else FileType.FOLDER
    val key = FileMetadataKeyWrapper(FileMetadataKey(
      filetype = filetype,
      path = file.toAbsolutePath.toString,
      changed = file.toFile.lastModified()))
    if (!rocks.exists(key)) {
      val hashList = backupFileOrFolder(file)
      rocks.write(key, hashList)
    }
    filesInRevision.append(key.fileMetadataKey)
  }

  def backupFolder(file: Path): FileMetadataValue = {
    fillInTypeInfo(file, None)
  }

  def fillInTypeInfo(file: Path, hashes: Option[BytesWrapper]): FileMetadataValue = {
    val attr = Files.readAttributes(file, classOf[BasicFileAttributes])
    val dosAttributes = Try(Files.readAttributes(file, classOf[DosFileAttributes])).toOption
    FileMetadataValue(
      filetype = if (file.toFile.isDirectory) FileType.FOLDER else FileType.FILE,
      length = attr.size(),
      created = attr.creationTime().toMillis,
      hashes = hashes.map(_.toProtobufByteString()).getOrElse(ByteString.EMPTY),
      dosIsReadonly = dosAttributes.exists(_.isReadOnly),
      dosIsArchive = dosAttributes.exists(_.isArchive),
      dosIsHidden = dosAttributes.exists(_.isHidden),
      dosIsSystem = dosAttributes.exists(_.isSystem),
    )
  }

  def backupFileOrFolder(file: Path): FileMetadataValue = {
    if (file.toFile.isDirectory) {
      backupFolder(file)
    } else {
      backupFile(file)
    }
  }

}

class MetadataExporter(rocksEnv: RocksEnv) extends Utils {
  val kvStore = rocksEnv.rocks
  private val filetype: StandardNumberedFileType = rocksEnv.fileManager.metadata

  def exportUpdates(): Unit = {
    val backupTime = new StandardMeasureTime()

    val contentValues = kvStore.readAllUpdates()

    val baos = new CustomByteArrayOutputStream()
    contentValues.foreach { case (_, value) =>
      baos.write(value.asArray())
    }
    for (valueLog <- new ValueLogWriter(rocksEnv, filetype, write = true, rocksEnv.config.volumeSize.bytes).autoClosed) {
      valueLog.write(baos.toBytesWrapper)
    }
    contentValues.foreach { case (key, _) =>
      kvStore.delete(key, writeAsUpdate = false)
    }
    logger.info(s"Finished exporting updates, took " + backupTime.measuredTime())
    kvStore.commit()
  }
}


class MetadataImporter(rocksEnv: RocksEnv) extends Utils {
  val kvStore = rocksEnv.rocks
  private val filetype: StandardNumberedFileType = rocksEnv.fileManager.metadata

  def importMetadata(): Unit = {
    val restoreTime = new StandardMeasureTime()

    for (file <- filetype.getFiles()) {
      for {
        reader <- rocksEnv.config.newReader(file).autoClosed
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
      val relativePath = rocksEnv.relativize(file)
      val key = ValueLogStatusKey(relativePath)
      val status = kvStore.readValueLogStatus(key)
      if (status.isEmpty) {
        logger.info("Adding status = finished for $relativePath")
        kvStore.write(key, ValueLogStatusValue(Status.FINISHED, file.length()))
      }
    }
    logger.info(s"Finished importing metadata, took " + restoreTime.measuredTime())
    kvStore.commit()
  }
}