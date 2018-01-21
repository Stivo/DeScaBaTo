package ch.descabato.core

import java.util.Objects
import java.util.concurrent.{ExecutorService, Executors}

import akka.actor.{ActorSystem, TypedActor, TypedProps}
import akka.stream.ActorMaterializer
import ch.descabato.core.actors._
import ch.descabato.core_old.{BackupFolderConfiguration, FileManager}
import ch.descabato.utils.Utils

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._

class Universe(val config: BackupFolderConfiguration) extends Utils with LifeCycle {

  Objects.requireNonNull(config)

  implicit val system = ActorSystem("Sys")
  implicit val materializer = ActorMaterializer()

  val cpuService: ExecutorService = Executors.newFixedThreadPool(Math.min(config.threads, 4))
  implicit val ex = ExecutionContext.fromExecutorService(cpuService)

  val fileManager = new FileManager(Set.empty, config)
  val context = new BackupContext(config, system, fileManager, ex)

  private val blockStorageProps: TypedProps[BlockStorage] = TypedProps.apply(classOf[BlockStorage], new BlockStorageActor(context))
  private val name = "blockStorageActor"
  val blockStorageActor: BlockStorage = TypedActor(system).typedActorOf(blockStorageProps.withTimeout(5.minutes), name)

  private def initActor() {
    val ref = TypedActor(system).getActorRefFor(blockStorageActor)
    ref ! ref.path
  }
  initActor()

  private val backupFileActorProps: TypedProps[MetadataActor] = TypedProps.apply[MetadataActor](classOf[BackupFileHandler], new MetadataActor(context))
  val backupFileActor: BackupFileHandler = TypedActor(system).typedActorOf(backupFileActorProps.withTimeout(5.minutes))

  val actors: Seq[LifeCycle] = Seq(backupFileActor, blockStorageActor)

  override def startup(): Future[Boolean] = {
    Future.sequence(actors.map(_.startup())).map(_.reduce(_ && _))
  }

  override def finish(): Future[Boolean] = {
    val out = Await.result(Future.sequence(actors.map(_.finish())).map(_.reduce(_ && _)), 1.minute)
    system.terminate()
    cpuService.shutdown()
    Future.successful(out)
  }
}
