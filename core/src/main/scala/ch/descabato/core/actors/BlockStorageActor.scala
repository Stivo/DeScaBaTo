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

class BlockStorageActor(val context: BackupContext) extends BlockStorage with JsonUser with Receiver {

  val logger = LoggerFactory.getLogger(getClass)

  val config: BackupFolderConfiguration = context.config

  private var hasChanged = false

  private var previous: FastHashMap[StoredChunk] = new FastHashMap[StoredChunk]()
  private var current: FastHashMap[StoredChunk] = new FastHashMap[StoredChunk]()

  private var toBeStored: FastHashMap[Boolean] = new FastHashMap[Boolean]()

  private var _writer: VolumeWriter = _

  private def writer = {
    if (_writer == null) {
      val value = TypedProps.apply[VolumeWriter](classOf[VolumeWriter], new VolumeWriteActor(context, context.fileManager.volume.nextFile()))
      _writer = TypedActor(context.actorSystem).typedActorOf(value.withTimeout(5.minutes))
    }
    _writer
  }


  def startup(): Future[Boolean] = {
    Future {
      val files = context.fileManager.volumeIndex.getFiles(config.folder)
      for (file <- files) {
        val seq = readJson[Seq[StoredChunk]](file)
        previous ++= seq.map(x => (x.hash, x))
      }
      true
    }
  }

  override def hasAlready(block: Block): Future[Boolean] = {
    val haveAlready = previous.safeContains(block.hash) || current.safeContains(block.hash) || toBeStored.safeContains(block.hash)
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
      case (storedChunk: StoredChunk, promise: Promise[Boolean]) =>
        current += storedChunk.hash -> storedChunk
        toBeStored -= storedChunk.hash
        promise.complete(Try(true))
      case x =>
        println(s"Got unknown message $x")
    }
  }

  override def save(block: Block): Future[Boolean] = {
    val promise = Promise.apply[Boolean]()
    writer.saveBlock(block).map { storedChunk =>
      context.actorSystem.actorSelection(path).resolveOne(1.minute).foreach {
        _ ! (storedChunk, promise)
      }
    }
    promise.future
  }

  def read(hash: Hash): Future[BytesWrapper] = {
    val chunk: StoredChunk = current.get(hash).orElse(previous.get(hash)).get
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
    Await.result(Future.sequence(_readers.values.map(_.finish())), 1.hour)
    _readers = Map.empty
    if (_writer != null) {
      Await.result(_writer.finish(), 1.hour)
    }
    if (hasChanged) {
      logger.info("Started writing blocks metadata")
      writeToJson(context.fileManager.volumeIndex.nextFile(), current.values.toSeq)
      logger.info("Done Writing blocks metadata")
    }
    Future.successful(true)
  }

}
