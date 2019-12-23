//package ch.descabato.rocks
//
//import java.io.File
//
//import ch.descabato.rocks.RocksEnv
//import ch.rocksbackup.protobuf.keys.Status
//import ch.rocksbackup.protobuf.keys.ValueLogIndex
//import com.typesafe.scalalogging.LazyLogging
//
//import scala.collection.immutable.TreeMap
//
//object Compaction {
//
//  def main(args: Array[String]): Unit = {
//    val rootFolder = "l:/"
//    val config = Config(command = Commands.Compaction, new File(rootFolder))
//    val env = RocksEnv(config)
//    compact(env)
//  }
//
//  def compact(env: RocksEnv): Unit = {
//    val chunks = env.rocks.getAllChunks()
//    val chunksAndIndexes = chunks.toMap
//    var chunkReferenceCount = Map.empty[ChunkKey, Int]
//    env.rocks.delete(Revision(2))
//    for ((key, _) <- chunks) {
//      chunkReferenceCount += key -> 0
//    }
//    val revisions = env.rocks.getAllRevisions()
//    for {
//      (_, value) <- revisions
//      chunks <- value.files.map(FileMetadataKeyWrapper).flatMap(env.rocks.readFileMetadata)
//      hash <- chunks.hashes.toByteArray.grouped(32)
//    } {
//      val key = ChunkKey(hash)
//      chunkReferenceCount += key -> (chunkReferenceCount(key) + 1)
//    }
//
//    val valueLogFileContentTracker = new ValueLogFileContentTracker(env.valuelogsFolder)
//    chunkReferenceCount.foreach { case (chunk, count) =>
//      val index = chunksAndIndexes(chunk)
//      val canDelete = count == 0
//      valueLogFileContentTracker.add(chunk, index, canDelete)
//    }
//
//    valueLogFileContentTracker.deleteUnnecessaryValueLogs(env)
//
//    valueLogFileContentTracker.report()
//  }
//
//
//}
//
//case class Range(from: Long, to: Long, canDelete: Boolean) {
//  def contains(range: Range): Boolean = {
//    this.from <= range.from && this.to >= range.to
//  }
//}
//
//class ValueLogFileContentTracker(folder: File) extends LazyLogging {
//  private var map: Map[String, ValueLogFileContent] = Map.empty
//
//  def add(chunkKey: ChunkKey, valueLogIndex: ValueLogIndex, canDelete: Boolean): Unit = {
//    val filename = valueLogIndex.filename
//    if (!map.contains(filename)) {
//      map += filename -> new ValueLogFileContent(filename, new File(folder, filename).length())
//    }
//    map(filename).addChunk(chunkKey, valueLogIndex, canDelete)
//  }
//
//  def report(): Unit = {
//    for ((name, content) <- map) {
//      logger.info(s"$name is used ${content.deletablePercentage}% and is complete? ${content.complete}")
//    }
//  }
//
//  def deleteUnnecessaryValueLogs(rocksEnv: RocksEnv): Unit = {
//    logger.info(s"Starting to delete unnecessary value logs")
//    val toDelete = map.values.filter(_.canBeDeletedCompletely)
//    toDelete.foreach { content =>
//      logger.info(s"Deleting ${content.filename}")
//      // Set them all to be deletable in rocksdb and delete the references to it
//      content.chunks.foreach { key =>
//        rocksEnv.rocks.delete(key.key)
//      }
//      val logStatusKey = ValueLogStatusKey(content.filename)
//      val status = rocksEnv.rocks.readValueLogStatus(logStatusKey).get
//      val newStatus = status.copy(status = Status.MARKED_FOR_DELETION)
//      rocksEnv.rocks.write(logStatusKey, newStatus)
//    }
//    rocksEnv.rocks.commit()
//    toDelete.foreach { content =>
//      new File(rocksEnv.valuelogsFolder, content.filename).delete()
//      val logStatusKey = ValueLogStatusKey(content.filename)
//      val status = rocksEnv.rocks.readValueLogStatus(logStatusKey).get
//      val newStatus = status.copy(status = Status.DELETED)
//      rocksEnv.rocks.write(logStatusKey, newStatus)
//      logger.info(s"Deleted ${content.filename}")
//    }
//    rocksEnv.rocks.commit()
//    logger.info(s"Finished deleting unnecessary value logs")
//  }
//
//}
//
//class ValueLogFileContent(val filename: String, val totalSize: Long) {
//  private var _chunks: TreeMap[Long, ChunkInfo] = TreeMap.empty
//  private var currentCount: Long = 0L
//  private var currentDeletableCount: Long = 0L
//
//  def addChunk(chunkKey: ChunkKey, valueLogIndex: ValueLogIndex, canDelete: Boolean): Unit = {
//    _chunks += valueLogIndex.from -> new ChunkInfo(chunkKey, valueLogIndex, canDelete)
//    currentCount += valueLogIndex.lengthCompressed
//    if (canDelete) {
//      currentDeletableCount += valueLogIndex.lengthCompressed
//    }
//  }
//
//  def chunks: Iterable[ChunkInfo] = _chunks.values
//
//  def deletablePercentage: Double = {
//    100.0 * (1 - currentDeletableCount / currentCount)
//  }
//
//  def complete: Boolean = {
//    currentCount == totalSize
//  }
//
//  def canBeDeletedCompletely: Boolean = {
//    currentDeletableCount == totalSize
//  }
//
//}
//
//class ChunkInfo(val key: ChunkKey, val index: ValueLogIndex, val canDelete: Boolean)