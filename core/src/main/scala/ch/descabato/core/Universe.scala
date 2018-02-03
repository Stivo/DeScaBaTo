package ch.descabato.core

import java.util.Objects
import java.util.concurrent.{ExecutorService, Executors}

import akka.actor.{ActorSystem, TypedActor, TypedProps}
import akka.stream.ActorMaterializer
import ch.descabato.core.actors._
import ch.descabato.core.config.BackupFolderConfiguration
import ch.descabato.core.util.FileManager
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

  private val chunkStorageProps: TypedProps[ChunkStorage] = TypedProps.apply(classOf[ChunkStorage], new ChunkStorageActor(context))
  private val name = "blockStorageActor"
  val chunkStorageActor: ChunkStorage = TypedActor(system).typedActorOf(chunkStorageProps.withTimeout(5.minutes), name)

  context.eventBus.subscribe(MySubscriber(TypedActor(system).getActorRefFor(journalHandler), journalHandler), MyEvent.globalTopic)

  private val metadataStorageProps: TypedProps[MetadataStorageActor] = TypedProps.apply[MetadataStorageActor](classOf[MetadataStorage], new MetadataStorageActor(context))
  val metadataStorageActor: MetadataStorage = TypedActor(system).typedActorOf(metadataStorageProps.withTimeout(5.minutes))

  private val compressorProps: TypedProps[Compressor] = TypedProps.apply[Compressor](classOf[Compressor], Compressors(config))
  val compressor: Compressor = TypedActor(system).typedActorOf(compressorProps.withTimeout(5.minutes))

  context.eventBus.subscribe(MySubscriber(TypedActor(system).getActorRefFor(metadataStorageActor), metadataStorageActor), MyEvent.globalTopic)

  val actors: Seq[LifeCycle] = Seq(metadataStorageActor, chunkStorageActor)

  override def startup(): Future[Boolean] = {
    Future.sequence(actors.map(_.startup())).map(_.reduce(_ && _))
  }

  override def finish(): Future[Boolean] = {
    if (!_finished) {
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
      _finished = true
    }
    Future.successful(true)
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
