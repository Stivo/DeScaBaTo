package ch.descabato.rocks

import better.files._
import ch.descabato.CustomByteArrayOutputStream
import ch.descabato.core.FileVisitorCollector
import ch.descabato.frontend.MultipleBackupConf
import ch.descabato.rocks.protobuf.keys.BackedupFileType
import ch.descabato.rocks.protobuf.keys.FileMetadataKey
import ch.descabato.rocks.protobuf.keys.FileMetadataValue
import ch.descabato.rocks.protobuf.keys.RevisionValue
import ch.descabato.utils.BytesWrapper
import ch.descabato.utils.Implicits._
import ch.descabato.utils.Streams.VariableBlockOutputStream
import com.google.protobuf.ByteString
import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.codec.Charsets

import java.io.ByteArrayOutputStream
import java.io.File
import java.io.FileInputStream
import java.io.IOException
import java.io.OutputStream
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.attribute.DosFileAttributes
import scala.collection.mutable
import scala.util.Try

object Backup {

  def listFiles(folder: File, ignoreFile: Option[File]): (Seq[Path], Seq[Path]) = {
    val visitor = new FileVisitorCollector(ignoreFile)
    visitor.walk(folder.toPath)
    (visitor.dirs, visitor.files)
  }

}

class RunBackup(rocksEnv: RocksEnv, backupConf: MultipleBackupConf) extends LazyLogging {

  def run(file: Seq[File]): Unit = {
    val backupper = new Backupper(rocksEnv)
    backupper.backup(file: _*)
    backupper.printStatistics()
    //    val totalSize = listFiles(dbFolder).map(_.toFile.length()).sum
    //    val files = listFiles(dbFolder).size
    //    println(s"Created $files files with ${Utils.readableFileSize(totalSize)}")
  }

}

class Backupper(rocksEnv: RocksEnv) extends LazyLogging {

  import rocksEnv._

  def printStatistics(): Unit = {
    logger.info(s"Chunks found: ${chunkFoundCounter}")
    logger.info(s"Chunks not found: ${chunkNotFoundCounter}")
    logger.info(StopWatch.report)
  }

  var revisionContent: ByteArrayOutputStream = new ByteArrayOutputStream()
  var filesInRevision: mutable.Buffer[FileMetadataKey] = mutable.Buffer.empty

  val valueLog = new ValueLogWriter(rocksEnv, rocksEnv.fileManager.volume, write = true, rocksEnv.config.volumeSize.bytes)
  val compressionDecider = CompressionDeciders.createForConfig(rocksEnv.config)

  def findNextRevision(): Int = {
    val iterator = rocks.getAllRevisionKeys()
    if (iterator.isEmpty) {
      1
    } else {
      iterator.map(_.number).max + 1
    }
  }

  def backup(str: File*): Unit = {
    val revision = Revision(findNextRevision())
    val mt = new StandardMeasureTime()
    RepairLogic.setStateTo(RocksStates.Writing, rocks)
    for (folderToBackup <- str) {
      val (folders, files) = Backup.listFiles(folderToBackup, rocksEnv.config.ignoreFile)
      for (file <- folders ++ files) {
        backupFileOrFolderIfNecessary(file)
      }
    }
    logger.info("Took " + mt.measuredTime())
    val configJson = new String(rocksEnv.config.asJson(), Charsets.UTF_8)
    val value = RevisionValue(created = System.currentTimeMillis(), configJson = configJson, files = filesInRevision.toSeq)
    rocks.write(revision, value)
    logger.info("Closing valuelog")
    valueLog.close()
    rocks.commit()
    new DbExporter(rocksEnv).exportUpdates()
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

  def backupChunk(file: File, bytesWrapper: BytesWrapper, hashList: OutputStream): Unit = {
    //    println(s"Backing up chunk with length ${toByteArray.length}")
    val hash = hashTiming.measure {
      digest.reset()
      digest.digest(bytesWrapper)
    }
    val chunkKey = ChunkKey(hash)
    if (rocks.exists(chunkKey)) {
      chunkFoundCounter += 1
    } else {
      val compressed = compressionDecider.compressBlock(file, bytesWrapper)
      val (index, commit) = valueLog.write(compressed)
      if (commit) {
        rocks.commit()
      }
      rocks.write(chunkKey, index)
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
        backupChunk(file.toFile, wrapper, hashList)
        logger.trace(s"Chunk $chunk with length ${wrapper.length}")
        chunk += 1
      }).autoClosed
    } {
      fis.pipeTo(chunker, buffer)
    }
    fillInTypeInfo(file, Some(hashList.toBytesWrapper))
  }

  private def backupFileOrFolderIfNecessary(file: Path): Unit = {
    val filetype = if (file.toFile.isFile) BackedupFileType.FILE else BackedupFileType.FOLDER
    val key = FileMetadataKeyWrapper(FileMetadataKey(
      filetype = filetype,
      path = file.toAbsolutePath.toString,
      changed = file.toFile.lastModified()))
    if (!rocks.exists(key)) {
      backupFileOrFolder(file).foreach { fileMetadataValue =>
        rocks.write(key, fileMetadataValue)
      }
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
      filetype = if (file.toFile.isDirectory) BackedupFileType.FOLDER else BackedupFileType.FILE,
      length = attr.size(),
      created = attr.creationTime().toMillis,
      hashes = hashes.map(_.toProtobufByteString()).getOrElse(ByteString.EMPTY),
      dosIsReadonly = dosAttributes.exists(_.isReadOnly),
      dosIsArchive = dosAttributes.exists(_.isArchive),
      dosIsHidden = dosAttributes.exists(_.isHidden),
      dosIsSystem = dosAttributes.exists(_.isSystem),
    )
  }

  def backupFileOrFolder(file: Path): Option[FileMetadataValue] = {
    if (file.toFile.isDirectory) {
      Some(backupFolder(file))
    } else {
      try {
        Some(backupFile(file))
      } catch {
        case e: IOException =>
          logger.error(s"Could not backup file $file", e)
          None
      }
    }
  }

}

