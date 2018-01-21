package ch.descabato.core

import java.util.Objects

import akka.actor.{ActorSystem, TypedActor, TypedProps}
import akka.stream.ActorMaterializer
import ch.descabato.akka.ActorStats.ex
import ch.descabato.core.actors._
import ch.descabato.core_old.{BackupFolderConfiguration, FileManager}
import ch.descabato.utils.Utils

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

class Universe(val config: BackupFolderConfiguration) extends Utils with LifeCycle {

  Objects.requireNonNull(config)

  implicit val system = ActorSystem("Sys")
  implicit val materializer = ActorMaterializer()

  val dispatcher = "backup-dispatcher"


  val eventBus = new MyEventBus()

  val fileManager = new FileManager(Set.empty, config)
  val context = new BackupContext(config, system, fileManager, ex, eventBus)

  private val blockStorageProps: TypedProps[BlockStorage] =
    TypedProps.apply(classOf[BlockStorage], new BlockStorageActor(context)).withDispatcher(dispatcher)
  private val name = "blockStorageActor"
  val blockStorageActor: BlockStorage = TypedActor(system).typedActorOf(blockStorageProps.withTimeout(5.minutes), name)

  private def initActor() {
    val ref = TypedActor(system).getActorRefFor(blockStorageActor)
    ref ! ref.path
  }

  initActor()

  private val backupFileActorProps: TypedProps[MetadataActor] =
    TypedProps.apply[MetadataActor](classOf[BackupFileHandler], new MetadataActor(context)).withDispatcher(dispatcher)
  val backupFileActor: BackupFileHandler = TypedActor(system).typedActorOf(backupFileActorProps.withTimeout(5.minutes))

  context.eventBus.subscribe(new MySubscriber(TypedActor(system).getActorRefFor(backupFileActor), backupFileActor), MyEvent.globalTopic)

  val actors: Seq[LifeCycle] = Seq(backupFileActor, blockStorageActor)

  override def startup(): Future[Boolean] = {
    Future.sequence(actors.map(_.startup())).map(_.reduce(_ && _))
  }

  override def finish(): Future[Boolean] = {
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
    system.terminate()
    //cpuService.shutdown()
    Future.successful(true)
  }
}
