package ch.descabato.core

import java.util.concurrent.{ExecutorService, Executors}

import akka.actor.{ActorSystem, TypedActor, TypedProps}
import akka.stream.ActorMaterializer
import ch.descabato.core.actors.{MetadataActor, BlockStorageIndexActor, ChunkHandler, ChunkStorageActor}
import ch.descabato.core_old.BackupFolderConfiguration
import ch.descabato.utils.Utils

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._

class Universe(val config: BackupFolderConfiguration) extends Utils with LifeCycle {

  implicit val system = ActorSystem("Sys")
  implicit val materializer = ActorMaterializer()

  val cpuService: ExecutorService = Executors.newFixedThreadPool(Math.min(config.threads, 4))
  implicit val ex = ExecutionContext.fromExecutorService(cpuService)

  private val chunkWriterProps: TypedProps[ChunkStorageActor] = TypedProps.apply[ChunkStorageActor](classOf[ChunkHandler], new ChunkStorageActor(config))
  val chunkWriter: ChunkHandler = TypedActor(system).typedActorOf(chunkWriterProps.withTimeout(5.minutes))

  private val blockStorageProps: TypedProps[BlockStorage] = TypedProps.apply(classOf[BlockStorage], new BlockStorageIndexActor(config, chunkWriter))
  val blockStorageActor: BlockStorage = TypedActor(system).typedActorOf(blockStorageProps.withTimeout(5.minutes))

  private val backupFileActorProps: TypedProps[MetadataActor] = TypedProps.apply[MetadataActor](classOf[BackupFileHandler], new MetadataActor(config))
  val backupFileActor: BackupFileHandler = TypedActor(system).typedActorOf(backupFileActorProps.withTimeout(5.minutes))

  val actors: Seq[LifeCycle] = Seq(backupFileActor, blockStorageActor, chunkWriter)

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
