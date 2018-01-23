package ch.descabato.core.actors

import java.io.File

import akka.actor.{TypedActor, TypedProps}
import ch.descabato.core.model.{Block, StoredChunk}
import ch.descabato.core.{BlockStorage, JsonUser}
import ch.descabato.core_old.BackupFolderConfiguration
import ch.descabato.frontend.{ProgressReporters, SizeStandardCounter}
import ch.descabato.utils.Implicits._
import ch.descabato.utils.{BytesWrapper, FastHashMap, Hash}
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success}

class BlockStorageActor(val context: BackupContext) extends BlockStorage with JsonUser {

  val logger = LoggerFactory.getLogger(getClass)

  val config: BackupFolderConfiguration = context.config

  private var hasChanged = false

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
    val file = context.fileManager.volume.nextFile()
    _currentWriter = new VolumeWriteActor(context, file)
  }

  def startup(): Future[Boolean] = {
    Future {
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

  override def hasAlready(hash: Hash): Future[Boolean] = {
    val haveAlready = checkpointed.safeContains(hash) || notCheckpointed.safeContains(hash)
    if (!haveAlready) {
      hasChanged = true
    }
    Future.successful(haveAlready)
  }

  private def rollVolume() = {
    currentWriter.finish()
    val filename = currentWriter.filename
    val toSave = notCheckpointed.filter { case (_, block) =>
      block.file == filename
    }
    writeToJson(context.fileManager.volumeIndex.nextFile(), toSave.values.toSeq)
    checkpointed ++= toSave
    notCheckpointed --= toSave.keySet
    logger.info(s"Wrote volume and index for $filename")
    context.eventBus.publish(VolumeRolled(filename))
    newWriter()
  }

  override def save(block: Block): Future[Boolean] = {
    if (shouldRollBeforeBlock(block)) {
      rollVolume()
    }
    val storedChunk = currentWriter.saveBlock(block)
    notCheckpointed += storedChunk.hash -> storedChunk
    bytesStoredCounter.current += storedChunk.length.size

    Future.successful(true)
  }

  private def shouldRollBeforeBlock(block: Block) = {
    currentWriter.currentPosition() + block.compressed.length + headroomInVolume > config.volumeSize.bytes
  }

  def read(hash: Hash): Future[BytesWrapper] = {
    val chunk: StoredChunk = notCheckpointed.get(hash).orElse(checkpointed.get(hash)).get
    Future(getReader(chunk).read(chunk))
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
      _currentWriter.finish()
    }
    Future.successful(true)
  }

  private def closeReaders() = {
    Await.result(Future.sequence(_readers.values.map(_.finish())), 1.hour)
    _readers = Map.empty
  }
}
