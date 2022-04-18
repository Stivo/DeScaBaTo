package ch.descabato.core.actions

import better.files._
import ch.descabato.core.FileVisitorCollector
import ch.descabato.core.model.BackupEnv
import ch.descabato.core.model.ChunkId
import ch.descabato.core.model.ChunkKey
import ch.descabato.core.model.FileMetadataId
import ch.descabato.core.model.RevisionKey
import ch.descabato.core.model.Size
import ch.descabato.core.util.InMemoryDb
import ch.descabato.core.util.ValueLogWriter
import ch.descabato.frontend.FileCounter
import ch.descabato.frontend.MaxValueCounter
import ch.descabato.frontend.ProgressReporters
import ch.descabato.frontend.SizeStandardCounter
import ch.descabato.frontend.StandardCounter
import ch.descabato.protobuf.keys.BackedupFileType
import ch.descabato.protobuf.keys.FileMetadataKey
import ch.descabato.protobuf.keys.FileMetadataValue
import ch.descabato.protobuf.keys.RevisionValue
import ch.descabato.utils.BytesWrapper
import ch.descabato.utils.Implicits._
import ch.descabato.utils.StandardMeasureTime
import ch.descabato.utils.StopWatch
import ch.descabato.utils.Streams.VariableBlockOutputStream
import com.typesafe.scalalogging.LazyLogging

import java.io.ByteArrayOutputStream
import java.io.File
import java.io.FileInputStream
import java.io.IOException
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.attribute.DosFileAttributes
import scala.collection.mutable
import scala.util.Try

object Backup {

  def listFiles(folder: File, ignoreFile: Option[File]): FileVisitorCollector = {
    val visitor = new FileVisitorCollector(ignoreFile)
    visitor.walk(folder.toPath)
    visitor
  }

}

class DoBackup(backupEnv: BackupEnv) extends LazyLogging {

  def run(file: Seq[File]): Unit = {
    val backupper = new Backupper(backupEnv)
    backupper.backup(file: _*)
    backupper.printStatistics()
  }

}

class ReuseCounter(override val name: String, val foundCounter: StandardCounter, val notFoundCounter: StandardCounter) extends MaxValueCounter {

  private var lastUpdateFound = 0L
  private var lastUpdateNotFound = 0L

  override def update(): Unit = {
    lastUpdateFound = foundCounter.current
    lastUpdateNotFound = notFoundCounter.current
    current = lastUpdateFound
    maxValue = lastUpdateNotFound + lastUpdateFound
    super.update()
  }

}

class Backupper(backupEnv: BackupEnv) extends LazyLogging {

  private val fileCounter = new FileCounter()
  private val byteCounter = new SizeStandardCounter(s"Size")

  private val chunkNotFoundCounter = new StandardCounter("New chunks")
  private val chunkFoundCounter = new StandardCounter("Found chunks")
  private val fileNotFoundCounter = new StandardCounter("New files")
  private val fileFoundCounter = new StandardCounter("Found files")
  private val reusedChunksCounter = new ReuseCounter("Reused chunks", chunkFoundCounter, chunkNotFoundCounter)
  private val reusedFilesCounter = new ReuseCounter("Reused files", fileFoundCounter, fileNotFoundCounter)

  import backupEnv._

  def printStatistics(): Unit = {
    logger.info(reusedChunksCounter.toString)
    logger.info(reusedFilesCounter.toString)
    logger.info(StopWatch.report)
  }

  var revisionContent: ByteArrayOutputStream = new ByteArrayOutputStream()
  var filesInRevision: mutable.Buffer[FileMetadataId] = mutable.Buffer.empty

  val valueLog = new ValueLogWriter(backupEnv, backupEnv.fileManager.volume, write = true, backupEnv.config.volumeSize.bytes)
  val compressionDecider: CompressionDecider = CompressionDeciders.createForConfig(backupEnv.config)

  def findNextRevision(): Int = {
    val revisions = rocks.getAllRevisions()
    if (revisions.isEmpty) {
      1
    } else {
      revisions.map(_._1.number).max + 1
    }
  }

  def backup(str: File*): Unit = {
    val revision = RevisionKey(findNextRevision())
    val mt = new StandardMeasureTime()
    ProgressReporters.addCounter(fileCounter, reusedChunksCounter, reusedFilesCounter)
    for (folderToBackup <- str) {
      val visitor = Backup.listFiles(folderToBackup, backupEnv.config.ignoreFile)
      logger.info(s"$str: Found ${visitor.fileCounter.maxValue} files to backup (${Size(visitor.bytesCounter.maxValue)}).")
      byteCounter.maxValue = visitor.bytesCounter.maxValue
      ProgressReporters.addCounter(visitor.fileCounter, byteCounter)
      for (file <- visitor.dirs) {
        backupFileOrFolderIfNecessary(file)
      }
      for (file <- visitor.files) {
        backupFileOrFolderIfNecessary(file)
        visitor.fileCounter += 1
      }
    }
    logger.info("Took " + mt.measuredTime())
    val configJson = new String(backupEnv.config.asJson(), StandardCharsets.UTF_8)
    val value = RevisionValue(created = System.currentTimeMillis(), configJson = configJson, fileIdentifiers = filesInRevision.toSeq)
    rocks.writeRevision(revision, value)
    logger.info("Closing valuelog")
    valueLog.close()
    InMemoryDb.writeFile(backupEnv, rocks.getUpdates())
  }

  private val buffer = Array.ofDim[Byte](1024 * 1024)
  private val digest = backupEnv.config.createMessageDigest()

  val hashTiming = new StopWatch("sha256")
  val buzhashTiming = new StopWatch("buzhash")

  def backupChunk(file: File, bytesWrapper: BytesWrapper, hashIds: mutable.Buffer[ChunkId]): Unit = {
    val hash = hashTiming.measure {
      digest.reset()
      digest.digest(bytesWrapper)
    }
    val chunkKey = ChunkKey(hash)

    val existing = rocks.getChunk(chunkKey)
    val newChunkId = if (existing.isDefined) {
      chunkFoundCounter += 1
      existing.get._1
    } else {
      val compressed = compressionDecider.compressBlock(file, bytesWrapper)
      val (_, index) = valueLog.write(compressed)
      // TODO progress reporting: Say something after a new volume has been written?
      //      if (newFile) {
      //        logger.info(s"Currently backing up ${file.getAbsolutePath}")
      //      }
      chunkNotFoundCounter += 1
      rocks.writeChunk(chunkKey, index)
    }
    hashIds += newChunkId
  }

  def backupFile(file: Path): FileMetadataValue = {
    val hashIds = mutable.Buffer.empty[ChunkId]
    fileCounter.fileName = file.toFile.toString
    fileCounter.maxValue = file.toFile.length()
    fileCounter.current = 0
    logger.debug(s"Backing up ${file.toFile.toString} (${Size(file.toFile.length())})")
    var chunk = 0
    for {
      fis <- new FileInputStream(file.toFile).autoClosed
      chunker <- new VariableBlockOutputStream({ wrapper =>
        backupChunk(file.toFile, wrapper, hashIds)
        fileCounter += wrapper.length
        byteCounter += wrapper.length
        logger.trace(s"Chunk $chunk with length ${wrapper.length}")
        chunk += 1
      }).autoClosed
    } {
      fis.pipeTo(chunker, buffer)
    }
    fillInTypeInfo(file, Some(hashIds.toSeq))
  }

  private def backupFileOrFolderIfNecessary(file: Path): Unit = {
    val filetype = if (file.toFile.isFile) BackedupFileType.FILE else BackedupFileType.FOLDER
    val key = FileMetadataKey(
      filetype = filetype,
      path = file.toAbsolutePath.toString,
      changed = file.toFile.lastModified())
    val existing = rocks.getFileMetadataByKey(key)
    if (existing.isEmpty) {
      if (filetype.isFile) {
        fileNotFoundCounter += 1
      }
      backupFileOrFolder(file).foreach { fileMetadataValue =>
        val newId = rocks.writeFileMetadata(key, fileMetadataValue)
        filesInRevision.append(newId)
      }
    } else {
      if (filetype.isFile) {
        fileFoundCounter += 1
      }
      filesInRevision.append(existing.get._1)
    }
  }

  def backupFolder(file: Path): FileMetadataValue = {
    fillInTypeInfo(file, None)
  }

  def fillInTypeInfo(file: Path, hashIds: Option[Seq[ChunkId]]): FileMetadataValue = {
    val attr = Files.readAttributes(file, classOf[BasicFileAttributes])
    val dosAttributes = Try(Files.readAttributes(file, classOf[DosFileAttributes])).toOption
    FileMetadataValue(
      filetype = if (file.toFile.isDirectory) BackedupFileType.FOLDER else BackedupFileType.FILE,
      length = attr.size(),
      created = attr.creationTime().toMillis,
      hashIds = hashIds.getOrElse(Seq.empty),
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

