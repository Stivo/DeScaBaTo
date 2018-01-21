package ch.descabato.core.actors

import java.io.File

import akka.actor.TypedActor.Receiver
import akka.actor.{ActorPath, ActorRef, TypedActor, TypedProps}
import ch.descabato.core.model.{Block, StoredChunk}
import ch.descabato.core.{BlockStorage, JsonUser}
import ch.descabato.core_old.BackupFolderConfiguration
import ch.descabato.utils.Implicits._
import ch.descabato.utils.{BytesWrapper, FastHashMap, Hash}
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future, Promise}
import scala.util.Try

class BlockStorageActor(val context: BackupContext) extends BlockStorage with JsonUser with TypedActor.Receiver {

  val logger = LoggerFactory.getLogger(getClass)

  val config: BackupFolderConfiguration = context.config

  private var hasChanged = false

  private var checkpointed: FastHashMap[StoredChunk] = new FastHashMap[StoredChunk]()
  private var notCheckpointed: FastHashMap[StoredChunk] = new FastHashMap[StoredChunk]()

  private var toBeStored: FastHashMap[Boolean] = new FastHashMap[Boolean]()

  class WriterInfos(val writer: VolumeWriter, val filename: String, var requestsSent: Int = 0, var requestsGot: Int = 0,
                    var bytesSent: Long = 0L, var finishRequested: Boolean = false, var indexWritten: Boolean = false) {

    private def writeIndex(): Unit = {
      if (!indexWritten) {
        val toSave = notCheckpointed.filter { case (_, block) =>
          block.file == filename
        }
        writeToJson(context.fileManager.volumeIndex.nextFile(), toSave.values.toSeq)
        checkpointed ++= toSave
        notCheckpointed --= toSave.keySet
        logger.info(s"Wrote volume and index for $filename with $requestsGot blocks")
        indexWritten = true
      }
    }

    def tryToFinish(): Unit = {
      if (canWriteIndex()) {
        writeIndex()
        context.eventBus.publish(new VolumeRolled(filename))
        writers -= filename
      }
    }

    private def canWriteIndex(): Boolean = {
      finishRequested && requestsSent == requestsGot
    }

    def requestFinish(): Unit = {
      finishRequested = true
      writer.finish().map(_ => sendToSelf(this))
    }

    override def toString = s"WriterInfos($filename, $requestsSent, $requestsGot, $finishRequested, $indexWritten)"
  }

  private var _currentWriter: WriterInfos = _
  private var writers: Map[String, WriterInfos] = Map.empty

  val headroomInVolume = 1000

  private def currentWriter = {
    if (_currentWriter == null) {
      newWriter()
    }
    _currentWriter
  }

  private def newWriter(): Unit = {
    val file = context.fileManager.volume.nextFile()
    val value: TypedProps[VolumeWriter] = TypedProps.apply[VolumeWriter](classOf[VolumeWriter], new VolumeWriteActor(context, file))
    val writer: VolumeWriter = TypedActor(context.actorSystem).typedActorOf(value.withTimeout(5.minutes))
    val name = writer.filename
    val infos = new WriterInfos(writer, name)
    writers += name -> infos
    _currentWriter = infos
  }

  def startup(): Future[Boolean] = {
    Future {
      val files = context.fileManager.volumeIndex.getFiles(config.folder)
      for (file <- files) {
        val seq = readJson[Seq[StoredChunk]](file)
        checkpointed ++= seq.map(x => (x.hash, x))
      }
      true
    }
  }

  override def hasAlready(block: Block): Future[Boolean] = {
    val haveAlready = checkpointed.safeContains(block.hash) || notCheckpointed.safeContains(block.hash) || toBeStored.safeContains(block.hash)
    if (!haveAlready) {
      toBeStored += block.hash -> true
      hasChanged = true
    }
    Future.successful(haveAlready)
  }

  var path: ActorPath = _

  override def onReceive(message: Any, sender: ActorRef): Unit = {
    message match {
      case x: ActorPath =>
        this.path = x
      case w: WriterInfos =>
        w.tryToFinish()
      case (storedChunk: StoredChunk, promise: Promise[Boolean]) =>
        val infos = writers(storedChunk.file)
        infos.requestsGot += 1
        notCheckpointed += storedChunk.hash -> storedChunk
        toBeStored -= storedChunk.hash
        infos.tryToFinish()
        promise.complete(Try(true))
      case x =>
        println(s"Got unknown message $x")
    }
  }

  private def rollVolume() = {
    currentWriter.requestFinish()
    newWriter()
  }

  override def save(block: Block): Future[Boolean] = {
    val promise = Promise.apply[Boolean]()
    if (shouldRollBeforeBlock(block)) {
      rollVolume()
    }
    currentWriter.bytesSent += block.content.length
    currentWriter.requestsSent += 1
    currentWriter.writer.saveBlock(block).map { storedChunk =>
      val tuple = (storedChunk, promise)
      sendToSelf(tuple)
    }
    promise.future
  }

  private def sendToSelf(msg: Any) = {
    context.actorSystem.actorSelection(path).resolveOne(1.minute).foreach {
      _ ! msg
    }
  }

  private def shouldRollBeforeBlock(block: Block) = {
    currentWriter.bytesSent + block.compressed.length + headroomInVolume > config.volumeSize.bytes
  }

  def read(hash: Hash): Future[BytesWrapper] = {
    val chunk: StoredChunk = notCheckpointed.get(hash).orElse(checkpointed.get(hash)).get
    getReader(chunk).read(chunk)
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
    writers.foreach(_._2.requestFinish())
    Future.successful(writers.isEmpty)
  }

  private def closeReaders() = {
    Await.result(Future.sequence(_readers.values.map(_.finish())), 1.hour)
    _readers = Map.empty
  }
}
