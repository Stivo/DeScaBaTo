package ch.descabato.core.actors

import java.io.File

import akka.actor.{TypedActor, TypedProps}
import ch.descabato.core._
import ch.descabato.core.model.{Block, ChunkIds, StoredChunk}
import ch.descabato.core_old.{BackupFolderConfiguration, Size, StandardMeasureTime}
import ch.descabato.frontend.{ProgressReporters, SizeStandardCounter}
import ch.descabato.utils.Implicits._
import ch.descabato.utils.{BytesWrapper, FastHashMap, Hash}
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success, Try}

class ChunkStorageActor(val context: BackupContext) extends ChunkStorage with JsonUser {

  val logger = LoggerFactory.getLogger(getClass)

  val config: BackupFolderConfiguration = context.config

  private var assignedIds: Map[Long, StoredChunk] = Map.empty
  private var alreadyAssignedIds: FastHashMap[Long] = new FastHashMap[Long]()
  private var checkpointed: FastHashMap[StoredChunk] = new FastHashMap[StoredChunk]()
  private var notCheckpointed: FastHashMap[StoredChunk] = new FastHashMap[StoredChunk]()

  private var _currentWriter: VolumeWriteActor = null

  val headroomInVolume = 1000

  private val bytesStoredCounter = new SizeStandardCounter {
    override def name: String = "Stored"
  }

  ProgressReporters.addCounter(bytesStoredCounter)

  private def currentWriter = {
    if (_currentWriter == null) {
      newWriter()
    }
    _currentWriter
  }

  private def newWriter(): Unit = {
    val index = context.fileManager.volumeIndex
    val file = index.nextFile()
    val indexNumber = index.numberOf(file)
    _currentWriter = new VolumeWriteActor(context, context.fileManager.volume.fileForNumber(indexNumber))
  }

  def startup(): Future[Boolean] = {
    Future {
      val measure = new StandardMeasureTime
      deleteVolumesWithoutIndexes()
      for (file <- context.fileManager.volumeIndex.getFiles()) {
        readJson[Seq[StoredChunk]](file) match {
          case Success(seq) =>
            checkpointed ++= seq.map(x => (x.hash, x))
          case Failure(_) =>
            logger.error(s"Could not read index $file, deleting it instead")
            file.delete()
        }
      }
      deleteVolumesWithoutIndexes()
      logger.info(s"Parsed jsons in ${measure.measuredTime()}")
      measure.startMeasuring()
      assignedIds = checkpointed.values.map(x => (x.id, x)).toMap
      if (assignedIds.nonEmpty) {
        ChunkIds.maxId(assignedIds.keySet.max)
      }
      logger.info(s"Reconstructing state completed in ${measure.measuredTime()} ")
      true
    }
  }

  private def deleteVolumesWithoutIndexes() = {
    val indexes = context.fileManager.volumeIndex.getFiles().map(x =>
      (context.fileManager.volumeIndex.numberOf(x), x)
    ).toMap
    val volumes = context.fileManager.volume.getFiles().map(x =>
      (context.fileManager.volume.numberOf(x), x)
    ).toMap
    for ((volumeIndex, volume) <- volumes) {
      if (!indexes.safeContains(volumeIndex)) {
        logger.warn(s"Deleting volume $volume because there is no index for it")
        volume.delete()
      }
    }
  }

  def chunkId(hash: Hash, assignIdIfNotFound: Boolean): Future[ChunkIdResult] = {
    Future.successful {
      chunkIdInternal(hash, assignIdIfNotFound)
    }
  }

  private def chunkIdInternal(hash: Hash, assignIdIfNotFound: Boolean) = {
    val existingId = checkpointed.get(hash).orElse(notCheckpointed.get(hash))
    val alreadyAssigned = alreadyAssignedIds.get(hash)
    (existingId, alreadyAssigned) match {
      case (Some(_), Some(_)) => throw new IllegalStateException()
      case (Some(chunk), None) => ChunkFound(chunk.id)
      case (None, Some(id)) => ChunkIdAssigned(id)
      case (None, None) =>
        if (assignIdIfNotFound) {
          val newId = ChunkIds.nextId()
          alreadyAssignedIds += hash -> newId
          ChunkIdAssigned(newId)
        } else {
          ChunkUnknown
        }
    }
  }

  def read(id: Long): Future[BytesWrapper] = {
    val maybeChunk = assignedIds.get(id)
    maybeChunk match {
      case Some(chunk) => Future.successful(getReader(chunk).read(chunk.asFilePosition()))
      case None => Future.failed(new NullPointerException(s"Could not find chunk for id $id"))
    }
  }

  override def getHashForId(id: Long): Future[Hash] = {
    Future.fromTry(Try {
      assignedIds(id).hash
    })
  }

  def hasAlready(id: Long): Future[Boolean] = {
    Future.successful {
      assignedIds.get(id).flatMap { chunk =>
        checkpointed.get(chunk.hash).orElse(notCheckpointed.get(chunk.hash)).map(_ => true)
      }.getOrElse(false)
    }
  }

  private def finishVolumeAndCreateIndex() = {
    val hashFuture = currentWriter.finish().flatMap(_ => currentWriter.md5Hash)
    val filename = currentWriter.filename
    val toSave = notCheckpointed.filter { case (_, block) =>
      block.file == filename
    }
    val hash = Await.result(hashFuture, 10.minutes)
    context.sendFileFinishedEvent(currentWriter.file, hash)
    val indexFile: File = computeIndexFileForVolume()
    writeToJson(indexFile, toSave.values.toSeq)
    checkpointed ++= toSave
    notCheckpointed --= toSave.keySet
    require(notCheckpointed.isEmpty)
    logger.info(s"Wrote volume and index for $filename")
    _currentWriter = null
  }

  private def computeIndexFileForVolume() = {
    val numberOfVolume = context.fileManager.volume.numberOf(currentWriter.file)
    val volumeIndex = context.fileManager.volumeIndex
    volumeIndex.fileForNumber(numberOfVolume)
  }

  override def save(block: Block, id: Long): Future[Boolean] = {
    if (notCheckpointed.get(block.hash).orElse(checkpointed.get(block.hash)).isEmpty) {
      require(alreadyAssignedIds(block.hash) == id)
      if (blockCanNotFitAnymoreIntoCurrentWriter(block)) {
        finishVolumeAndCreateIndex()
      }
      val filePosition = currentWriter.saveBlock(block)
      val storedChunk = StoredChunk(id, currentWriter.filename, block.hash, filePosition.offset, filePosition.length)
      require(!assignedIds.safeContains(id), s"Should not contain $id")
      alreadyAssignedIds -= block.hash
      assignedIds += id -> storedChunk
      notCheckpointed += storedChunk.hash -> storedChunk
      bytesStoredCounter += storedChunk.length
    } else {
      logger.warn(s"Chunk with hash ${block.hash} was already saved, but was compressed another time anyway")
    }
    Future.successful(true)
  }

  private def blockCanNotFitAnymoreIntoCurrentWriter(block: Block) = {
    currentWriter.currentPosition() + block.compressed.length + headroomInVolume > config.volumeSize.bytes
  }

  private var _readers: Map[String, VolumeReader] = Map.empty

  def getReader(chunk: StoredChunk) = {
    if (!_readers.safeContains(chunk.file)) {
      val value = TypedProps.apply[VolumeReader](classOf[VolumeReader], new VolumeReadActor(context, new File(config.folder, chunk.file)))
      _readers += chunk.file -> TypedActor(context.actorSystem).typedActorOf(value.withTimeout(5.minutes))
    }
    _readers(chunk.file)
  }

  override def finish(): Future[Boolean] = {
    closeReaders()
    if (_currentWriter != null) {
      finishVolumeAndCreateIndex()
    }
    logger.info(s"Wrote volumes with total of ${Size(bytesStoredCounter.current)}")
    Future.successful(true)
  }

  private def closeReaders() = {
    Await.result(Future.sequence(_readers.values.map(_.finish())), 1.hour)
    _readers = Map.empty
  }
}
