package ch.descabato.core.actors

import java.io.File

import akka.actor.{TypedActor, TypedProps}
import ch.descabato.core._
import ch.descabato.core.commands.ProblemCounter
import ch.descabato.core.config.BackupFolderConfiguration
import ch.descabato.core.model._
import ch.descabato.frontend.{ETACounter, ProgressReporters, StandardByteCounter}
import ch.descabato.utils.Implicits._
import ch.descabato.utils._
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success, Try}

class ChunkStorageActor(val context: BackupContext, val journalHandler: JournalHandler) extends ChunkStorage with JsonUser {

  val logger = LoggerFactory.getLogger(getClass)

  val config: BackupFolderConfiguration = context.config

  private var assignedIds: Map[Long, StoredChunk] = Map.empty
  private var alreadyAssignedIds: FastHashMap[Long] = new FastHashMap[Long]()
  private var checkpointed: FastHashMap[StoredChunk] = new FastHashMap[StoredChunk]()
  private var notCheckpointed: FastHashMap[StoredChunk] = new FastHashMap[StoredChunk]()

  private var _currentWriter: (VolumeWriteActor, File) = null

  val headroomInVolume = 1000

  private val bytesStoredCounter = new StandardByteCounter("Stored")

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
    val indexNumber = index.numberOfFile(file)
    val volumeFile = context.fileManager.volume.fileForNumber(indexNumber)
    _currentWriter = (new VolumeWriteActor(context, volumeFile), volumeFile)
  }

  def startup(): Future[Boolean] = {
    Future {
      val measure = new StandardMeasureTime
      val files = context.fileManager.volumeIndex.getFiles()
      val futures = files.map { f =>
        Future {
          (f, readJson[Seq[StoredChunk]](f))
        }
      }
      for (elem <- futures) {
        Await.result(elem, 1.minute) match {
          case (_, Success(seq)) =>
            checkpointed ++= seq.map(x => (x.hash, x))
          case (f, Failure(_)) =>
            logger.warn(s"Could not read index $f, it might be corrupted, some data loss will occur. Backup again.")
        }
      }
      logger.info(s"Reconstructed state after ${measure.measuredTime()}")
      measure.startMeasuring()
      assignedIds = checkpointed.values.map(x => (x.id, x)).toMap
      if (assignedIds.nonEmpty) {
        ChunkIds.maxId(assignedIds.keySet.max)
      }
      logger.info(s"Reconstructing state completed in ${measure.measuredTime()}, have ${assignedIds.size} chunks")
      true
    }
  }

  def chunkId(block: Block, assignIdIfNotFound: Boolean): Future[ChunkIdResult] = {
    Future.successful {
      chunkIdInternal(block.hash, assignIdIfNotFound)
    }
  }

  private def chunkIdInternal(hash: Hash, assignIdIfNotFound: Boolean): ChunkIdResult = {
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
    val hashFuture = currentWriter._1.finish().flatMap(_ => currentWriter._1.md5Hash)
    val filename = currentWriter._1.filename
    val toSave = notCheckpointed.filter { case (_, block) =>
      block.file == filename
    }
    val hash = Await.result(hashFuture, 10.minutes)
    journalHandler.addFileToJournal(currentWriter._2, hash)
    val indexFile: File = computeIndexFileForVolume()
    writeToJson(indexFile, toSave.values.toSeq)
    checkpointed ++= toSave
    notCheckpointed --= toSave.keySet
    context.eventBus.publish(CheckpointedChunks(checkpointed.values.map(_.id).toSet))
    require(notCheckpointed.isEmpty)
    logger.info(s"Wrote volume and index for $filename")
    _currentWriter = null
  }

  private def computeIndexFileForVolume() = {
    val numberOfVolume = context.fileManager.volume.numberOfFile(currentWriter._2)
    val volumeIndex = context.fileManager.volumeIndex
    volumeIndex.fileForNumber(numberOfVolume)
  }

  override def save(block: CompressedBlock, id: Long): Future[Boolean] = {
    if (notCheckpointed.get(block.hash).orElse(checkpointed.get(block.hash)).isEmpty) {
      require(alreadyAssignedIds(block.hash) == id)
      if (blockCanNotFitAnymoreIntoCurrentWriter(block)) {
        finishVolumeAndCreateIndex()
      }
      val filePosition = currentWriter._1.saveBlock(block)
      val storedChunk = StoredChunk(id, currentWriter._1.filename, block.hash, filePosition.offset, filePosition.length)
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

  private def blockCanNotFitAnymoreIntoCurrentWriter(block: CompressedBlock) = {
    currentWriter._1.currentPosition() + block.compressed.length + headroomInVolume > config.volumeSize.bytes
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
    if (bytesStoredCounter.current > 0) {
      logger.info(s"Wrote volumes with total of ${Size(bytesStoredCounter.current)}")
    }
    Future.successful(true)
  }

  private def closeReaders() = {
    Await.result(Future.sequence(_readers.values.map(_.finish())), 1.hour)
    _readers = Map.empty
  }

  val verifiedCounter = new ETACounter {
    override def name: String = "Verified chunks"
  }

  override def verifyChunksAreAvailable(chunkIdsToTest: Seq[Long], counter: ProblemCounter, checkVolumeToo: Boolean, checkContent: Boolean): BlockingOperation = {
    ProgressReporters.addCounter(verifiedCounter)
    var futures = Seq.empty[Future[Unit]]
    val distinctIds = chunkIdsToTest.distinct
    verifiedCounter.maxValue = distinctIds.size
    for (chunkId <- distinctIds) {
      var shouldCount = true
      if (!assignedIds.safeContains(chunkId)) {
        counter.addProblem(s"Chunk with id $chunkId could not be found")
      } else {
        if (checkVolumeToo) {
          val chunk = assignedIds(chunkId)
          val file = config.resolveRelativePath(chunk.file)
          if (file.length() < (chunk.startPos + chunk.length)) {
            counter.addProblem(s"Chunk ${chunk.id} should be in file ${chunk.file} at ${chunk.startPos} - to ${chunk.startPos + chunk.length}, but file is only ${file.length} long")
          } else {
            if (checkContent) {
              val wrapper = getReader(chunk).read(chunk.asFilePosition())
              shouldCount = false
              futures :+= Future {
                val stream = CompressedStream.decompressToBytes(wrapper)
                val hashComputed: Hash = config.createMessageDigest().digest(stream)
                if (hashComputed !== chunk.hash) {
                  counter.addProblem(s"Chunk ${chunk} was read from volume and does not have the same hash as stored")
                }
                verifiedCounter += 1
              }
            }
          }
        }
      }
      if (shouldCount) {
        verifiedCounter += 1
      }
    }
    for (future <- futures) {
      Await.result(future, 1.hour)
    }
    new BlockingOperation()
  }

}
