package ch.descabato.rocks

import java.io.File

import better.files._
import ch.descabato.core.config.BackupFolderConfiguration
import ch.descabato.rocks.protobuf.keys.Status
import ch.descabato.rocks.protobuf.keys.ValueLogIndex
import ch.descabato.utils.Utils
import com.typesafe.scalalogging.LazyLogging

import scala.collection.immutable.TreeMap

object Compaction {

  def main(args: Array[String]): Unit = {
    val config = BackupFolderConfiguration(new File(""))
    val env = RocksEnv(config, false)
    val compaction = new Compaction(env, false)
    compaction.deleteUnreachableMetadata()
    compaction.deleteChunksPointingToMissingVolumes()
    compaction.deleteUnusedChunks()
    compaction.deleteUnusedVolumes()
  }
}

class Compaction(env: RocksEnv, dryRun: Boolean) extends Utils {

  def deleteUnreachableMetadata(): Unit = {
    var fileMetadatas = env.rocks.getAllFileMetadatas().map(x => (x._1, 0)).toMap.withDefaultValue(0)
    for {
      revision <- env.rocks.getAllRevisions()
      key <- revision._2.files
    } {
      fileMetadatas += key -> (fileMetadatas(key) + 1)
    }
    val toDelete = fileMetadatas.filter(_._2 == 0).keys.toSeq.sortBy(_.path)
    logger.info(s"Can delete all these ${toDelete.size} metadatas:\n" + toDelete.mkString("\n"))
    if (!dryRun && toDelete.nonEmpty) {
      for (key <- toDelete) {
        env.rocks.delete(FileMetadataKeyWrapper(key))
      }
      env.rocks.commit()
    }
  }

  def deleteChunksPointingToMissingVolumes(): Unit = {
    val existingFiles: Set[String] = existingVolumes()
    val chunksToDelete = for {
      chunk <- env.rocks.getAllChunks()
      if !existingFiles.contains(chunk._2.filename)
    } yield chunk
    logger.info(s"Can delete ${chunksToDelete.size} chunks because their volumes do not exist")
    if (!dryRun) {
      for (chunk <- chunksToDelete) {
        env.rocks.delete(chunk._1)
      }
      env.rocks.commit()
    }
  }

  private def existingVolumes() = {
    val volume = env.fileManager.volume
    val existingFiles = volume.getFiles().filter(x => !volume.isTempFile(x)).map(x => env.config.relativePath(x)).toSet
    existingFiles
  }

  def deleteUnusedChunks(): Unit = {
    var chunkReferenceCount = Map.empty[ChunkKey, Int]
    for ((key, _) <- env.rocks.getAllChunks()) {
      chunkReferenceCount += key -> 0
    }
    for {
      (_, value) <- env.rocks.getAllFileMetadatas()
      key <- env.rocks.getHashes(value)
    } {
      chunkReferenceCount += key -> (chunkReferenceCount(key) + 1)
    }
    val keysToDelete = chunkReferenceCount.filter(_._2 == 0).keys
    logger.info(s"Can delete following ${keysToDelete.size} chunks:\n" + keysToDelete.mkString("\n"))
    if (!dryRun && keysToDelete.nonEmpty) {
      keysToDelete.foreach {
        env.rocks.delete(_)
      }
      env.rocks.commit()
    }
  }

  def deleteUnusedVolumes(): Unit = {
    val chunks = env.rocks.getAllChunks()
    val tracker = new ValueLogFileContentTracker(env.rocksEnvInit, dryRun)
    for {
      (key, value) <- chunks
    } {
      tracker.add(key, value, false)
    }
    tracker.deleteUnnecessaryValueLogs(env)
    tracker.report()
  }

}

case class Range(from: Long, to: Long, canDelete: Boolean) {
  def contains(range: Range): Boolean = {
    this.from <= range.from && this.to >= range.to
  }
}

class ValueLogFileContentTracker(rocksEnv: RocksEnvInit, dryRun: Boolean) extends LazyLogging {
  private var map: Map[String, ValueLogFileContent] = Map.empty

  rocksEnv.fileManager.volume
    .getFiles()
    .filterNot(rocksEnv.fileManager.volume.isTempFile)
    .foreach { volume =>
      val relative = rocksEnv.config.relativePath(volume)
      map += relative -> new ValueLogFileContent(relative, rocksEnv.config)
    }

  def add(chunkKey: ChunkKey, valueLogIndex: ValueLogIndex, canDelete: Boolean): Unit = {
    val filename = valueLogIndex.filename
    if (!map.contains(filename)) {
      map += filename -> new ValueLogFileContent(filename, rocksEnv.config)
    }
    map(filename).addChunk(chunkKey, valueLogIndex, canDelete)
  }

  def report(): Unit = {
    for ((name, content) <- map.toSeq.sortBy(_._1)) {
      logger.info(s"$name is used ${content.deletablePercentage}% and is complete? ${content.complete}")
    }
  }

  def deleteUnnecessaryValueLogs(rocksEnv: RocksEnv): Unit = {
    logger.info(s"Starting to delete unnecessary value logs")
    val toDelete = map.values.filter(_.canBeDeletedCompletely).toSeq.sortBy(_.filename)
    toDelete.foreach { content =>
      logger.info(s"Deleting ${content.filename}")
      if (!dryRun) {
        val logStatusKey = ValueLogStatusKey(content.filename)
        val status = rocksEnv.rocks.readValueLogStatus(logStatusKey).get
        val newStatus = status.copy(status = Status.MARKED_FOR_DELETION)
        rocksEnv.rocks.write(logStatusKey, newStatus)
      }
    }
    if (!dryRun) {
      rocksEnv.rocks.commit()
      toDelete.foreach { content =>
        rocksEnv.config.resolveRelativePath(content.filename).delete()
        val logStatusKey = ValueLogStatusKey(content.filename)
        val status = rocksEnv.rocks.readValueLogStatus(logStatusKey).get
        val newStatus = status.copy(status = Status.DELETED)
        rocksEnv.rocks.write(logStatusKey, newStatus)
        logger.info(s"Deleted ${content.filename}")
      }
      rocksEnv.rocks.commit()
    }
    logger.info(s"Finished deleting unnecessary value logs")
  }

}

class ValueLogFileContent(val filename: String, backupFolderConfiguration: BackupFolderConfiguration) {
  private val file = backupFolderConfiguration.resolveRelativePath(filename)
  val totalSize: Long = file.length
  private var _chunks: TreeMap[Long, ChunkInfo] = TreeMap.empty
  private var currentCount: Long = 0L
  private var currentDeletableCount: Long = 0L

  private val startOfContent: Long = {
    var out = -1L
    if (file.exists()) {
      for (reader <- backupFolderConfiguration.newReader(file).autoClosed) {
        out = reader.startOfContent
      }
    }
    out
  }

  def addChunk(chunkKey: ChunkKey, valueLogIndex: ValueLogIndex, canDelete: Boolean): Unit = {
    _chunks += valueLogIndex.from -> new ChunkInfo(chunkKey, valueLogIndex, canDelete)
    currentCount += valueLogIndex.lengthCompressed
    if (canDelete) {
      currentDeletableCount += valueLogIndex.lengthCompressed
    }
  }

  def chunks: Iterable[ChunkInfo] = _chunks.values

  def deletablePercentage: Double = {
    if (currentCount == 0) {
      0
    } else {
      100.0 * (1 - currentDeletableCount / currentCount)
    }
  }

  def complete: Boolean = {
    currentCount + startOfContent == totalSize
  }

  def canBeDeletedCompletely: Boolean = {
    currentCount == currentDeletableCount
  }

}

class ChunkInfo(val key: ChunkKey, val index: ValueLogIndex, val canDelete: Boolean)