package ch.descabato.rocks

import java.io.ByteArrayOutputStream
import java.io.File
import java.io.FileInputStream
import java.io.OutputStream
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.attribute.DosFileAttributes
import java.security.MessageDigest

import better.files._
import ch.descabato.CustomByteArrayOutputStream
import ch.descabato.core.config.BackupFolderConfiguration
import ch.descabato.frontend.BackupConf
import ch.descabato.rocks.protobuf.keys.FileMetadataKey
import ch.descabato.rocks.protobuf.keys.FileMetadataValue
import ch.descabato.rocks.protobuf.keys.FileType
import ch.descabato.rocks.protobuf.keys.RevisionValue
import ch.descabato.utils.BytesWrapper
import ch.descabato.utils.Implicits._
import ch.descabato.utils.Streams.VariableBlockOutputStream
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

  def printStatistics(): Unit = {
    logger.info(s"Chunks found: ${chunkFoundCounter}")
    logger.info(s"Chunks not found: ${chunkNotFoundCounter}")
    logger.info(StopWatch.report)
  }

  var revisionContent: ByteArrayOutputStream = new ByteArrayOutputStream()
  var filesInRevision: mutable.Buffer[FileMetadataKey] = mutable.Buffer.empty

  val valueLog = new ValueLogWriter(rocksEnv, write = true, backupConf.volumeSize().bytes)

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

    rocks.currentRevision = revision
    for (folderToBackup <- str) {
      for (file <- Backup.listFiles(folderToBackup)) {
        backupFileOrFolderIfNecessary(file)
      }
    }
    logger.info("Took " + mt.measuredTime())
    val value = RevisionValue(System.currentTimeMillis(), filesInRevision.toSeq)
    rocks.write(revision, value)
    logger.info("Closing valuelog")
    valueLog.close()
    rocks.commit()
    backupRocksDb(revision)
    val compacting = new StandardMeasureTime()
    rocks.compact()
    logger.info("Compaction took " + compacting.measuredTime())
  }

  def backupRocksDb(revision: Revision): Unit = {
    val backupTime = new StandardMeasureTime()

    // TODO reactivate
    //    val filename = s"revision_${revision.number}.log"
    //    val statusKey = ValueLogStatusKey(filename)
    //    val statusValue = ValueLogStatusValue(Status.WRITING)
    //    kvStore.write(statusKey, statusValue)
    //    kvStore.commit()
    //    val file = new File(valuesFolder, filename)
    //    val fos = new FileOutputStream(file)
    //    items.foreach { case (_, value) =>
    //      fos.write(value.asArray(false))
    //    }
    //    fos.close()
    //    kvStore.write(statusKey, statusValue.copy(status = Status.FINISHED, size = file.length()))
    //    kvStore.commit()
    //    items.foreach { case (key, _) =>
    //      kvStore.delete(key)
    //    }
    //    println(s"Finished backing up revision $revision, took " + backupTime.measuredTime())
    // Verify keys are written correctly
    //    val fis = new FileInputStream(file)
    //    var x: Option[RevisionContentValue] = None
    //    do {
    //      x = RevisionContentValue.readNextEntry(fis)
    //      x.foreach { y =>
    //        println(y)
    //        println(kvStore.decodeRevisionContentValue(y))
    //      }
    //    } while (x.isDefined)
  }

  private val buffer = Array.ofDim[Byte](1024 * 1024)
  private val sha256 = MessageDigest.getInstance("SHA-256")

  var chunkNotFoundCounter = 0
  var chunkFoundCounter = 0

  val hashTiming = new StopWatch("sha256")
  val buzhashTiming = new StopWatch("buzhash")

  def backupChunk(bytesWrapper: BytesWrapper, hashList: OutputStream): Unit = {
    //    println(s"Backing up chunk with length ${toByteArray.length}")
    val hash = hashTiming.measure {
      sha256.reset()
      sha256.digest(bytesWrapper)
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
