package ch.descabato.core

import java.util.Objects
import java.util.concurrent.{ExecutorService, Executors}

import akka.actor.{ActorSystem, TypedActor, TypedProps}
import akka.stream.ActorMaterializer
import ch.descabato.core.actors._
import ch.descabato.core.config.BackupFolderConfiguration
import ch.descabato.core.util.FileManager
import ch.descabato.frontend.ProgressReporters
import ch.descabato.remote.{RemoteHandler, SimpleRemoteHandler}
import ch.descabato.utils.Utils

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

class Universe(val config: BackupFolderConfiguration) extends Utils with LifeCycle {

  Objects.requireNonNull(config)

  implicit val system = ActorSystem("Sys")
  implicit val materializer = ActorMaterializer()

  private var _finished = false
  private var _shutdown = false

  val cpuService: ExecutorService = Executors.newFixedThreadPool(Math.min(config.threads, 8))
  implicit val ex = ExecutionContext.fromExecutorService(cpuService)

  val eventBus = new MyEventBus()

  val fileManagerNew = new FileManager(config)
  val context = new BackupContext(config, system, fileManagerNew, ex, eventBus)

  private val journalHandlerProps: TypedProps[JournalHandler] = TypedProps.apply(classOf[JournalHandler], new SimpleJournalHandler(context))
  val journalHandler: JournalHandler = TypedActor(system).typedActorOf(journalHandlerProps.withTimeout(5.minutes))

  private val chunkStorageProps: TypedProps[ChunkStorage] = TypedProps.apply(classOf[ChunkStorage], new ChunkStorageActor(context, journalHandler))
  private val name = "blockStorageActor"
  val chunkStorageActor: ChunkStorage = TypedActor(system).typedActorOf(chunkStorageProps.withTimeout(5.minutes), name)

  private val metadataStorageProps: TypedProps[MetadataStorageActor] = TypedProps.apply[MetadataStorageActor](classOf[MetadataStorage], new MetadataStorageActor(context, journalHandler))
  val metadataStorageActor: MetadataStorage = TypedActor(system).typedActorOf(metadataStorageProps.withTimeout(5.minutes))

  private val compressorProps: TypedProps[Compressor] = TypedProps.apply[Compressor](classOf[Compressor], Compressors(config))
  val compressor: Compressor = TypedActor(system).typedActorOf(compressorProps.withTimeout(5.minutes))

  context.eventBus.subscribe(MySubscriber(TypedActor(system).getActorRefFor(metadataStorageActor), metadataStorageActor), MyEvent.globalTopic)

  val actors = Seq(metadataStorageActor, chunkStorageActor)

  val remoteActorOption: Option[RemoteHandler] = {
    if (config.remoteOptions.enabled) {
      val remoteActorProps: TypedProps[RemoteHandler] = TypedProps.apply[RemoteHandler](classOf[RemoteHandler], new SimpleRemoteHandler(context, journalHandler))
      val remoteActor: RemoteHandler = TypedActor(system).typedActorOf(remoteActorProps)
      context.eventBus.subscribe(MySubscriber(TypedActor(system).getActorRefFor(remoteActor), remoteActor), MyEvent.globalTopic)
      Some(remoteActor)
    } else {
      None
    }
  }

  override def startup(): Future[Boolean] = {
    val allActors = actors ++ remoteActorOption
    Future.sequence(allActors.map(_.startup())).map(_.reduce(_ && _))
  }

  override def finish(): Future[Boolean] = {
    if (!_finished) {
      waitForNormalActorsToFinish()
      waitForRemoteActorToFinish()
      _finished = true
    }
    Future.successful(true)
  }

  private def waitForNormalActorsToFinish() = {
    var actorsToDo: Seq[LifeCycle] = actors
    while (actorsToDo.nonEmpty) {
      val futures = actorsToDo.map(x => (x, x.finish()))
      actorsToDo = Seq.empty
      for ((actor, future) <- futures) {
        val hasFinished = Await.result(future, 1.minute)
        if (!hasFinished) {
          logger.info("One actor can not finish yet " + actor)
          actorsToDo :+= actor
        }
      }
      Thread.sleep(500)
    }
  }

  private def waitForRemoteActorToFinish() = {
    ProgressReporters.activeCounters = Seq(config.remoteOptions.uploaderCounter1)
    remoteActorOption match {
      case Some(remote) =>
        var isFinished = false
        do {
          isFinished = Await.result(remote.finish(), 1.minute)
          if (!isFinished) {
            Thread.sleep(1000)
          }
        } while (!isFinished)
      case None =>
        // nothing to do
    }
  }

  def shutdown(): Unit = {
    if (!_shutdown) {
      Await.result(finish(), 1.minute)
      journalHandler.finish()
      system.terminate()
      cpuService.shutdown()
      _shutdown = true
    }
  }
}
