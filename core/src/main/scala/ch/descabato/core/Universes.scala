package ch.descabato.core

import java.security.MessageDigest
import ch.descabato.CompressionMode
import ch.descabato.utils.CompressedStream
import akka.actor._
import scala.concurrent.future
import scala.concurrent.Future
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.reflect.ClassTag
import scala.concurrent.ExecutionContext

object Universes {

}

trait UniversePart {
  protected var universe: Universe = null
  def fileManager = universe.fileManager
  def config = universe.config
  def setup(universe: Universe) {
    this.universe = universe
    setupInternal()
  }

  protected def setupInternal() {}
}

class SingleThreadUniverse(val config: BackupFolderConfiguration) extends Universe {
  lazy val backupPartHandler = {
    val out = new ZipBackupPartHandler()
    out.setup(this)
    out
  }
  lazy val hashListHandler = {
    val out = new ZipHashListHandler()
    out.setup(this)
    out
  }
  lazy val cpuTaskHandler = new SingleThreadCpuTaskHandler(this)
  lazy val blockHandler = {
    val out = new ZipBlockHandler()
    out.setup(this)
    out
  }
}

class SingleThreadCpuTaskHandler(universe: Universe) extends CpuTaskHandler {

  def computeHash(content: Array[Byte], hashMethod: String, blockId: BlockId) {
    val md = MessageDigest.getInstance(hashMethod)
    universe.backupPartHandler.hashComputed(blockId, md.digest(content), content)
  }

  def compress(hash: Array[Byte], content: Array[Byte], method: CompressionMode, disable: Boolean) {
    val (header, compressed) = CompressedStream.compress(content, method, disable)
    universe.blockHandler.writeCompressedBlock(hash, header, compressed)
  }

  def finish = true

}

class FutureCpuTaskHandler(universe: Universe)(implicit context: ExecutionContext = ExecutionContext.Implicits.global) extends CpuTaskHandler {
  @volatile var futures: Vector[Future[_]] = Vector()

  def finish() = {
    var head: Future[_] = null
    while (!futures.isEmpty) {
      futures.synchronized {
        head = futures.head
        futures = futures.tail
      }
      Await.result(head, 10 minutes)
    }
    true
  }
  def computeHash(content: Array[Byte], hashMethod: String, blockId: BlockId) {
    futures.synchronized {
      cleanFutures()
      futures :+= future {
        val md = MessageDigest.getInstance(hashMethod)
        universe.backupPartHandler.hashComputed(blockId, md.digest(content), content)
      }
    }
  }

  def compress(hash: Array[Byte], content: Array[Byte], method: CompressionMode, disable: Boolean) {
    futures.synchronized {
      cleanFutures()
      futures :+= future {
        val (header, compressed) = CompressedStream.compress(content, method, disable)
        universe.blockHandler.writeCompressedBlock(hash, header, compressed)
      }
    }
  }
  
  def cleanFutures() {
    while (!futures.isEmpty && futures.head.isCompleted)
      futures = futures.tail
  }

}

class AkkaUniverse(val config: BackupFolderConfiguration) extends Universe {
  var system = ActorSystem("HelloSystem")

  def actorOf[T <: AnyRef: ClassTag] = TypedActor(system).typedActorOf(p(TypedProps.apply[T]))
  
  def p[T<: AnyRef](props: TypedProps[T]) = props.withDispatcher("backup-dispatcher").withTimeout(None)
  
  //  val computer = system.actorOf(Props[AkkaCpuTaskHandler].withMailbox("ComputerMailbox")
  //      .withDispatcher("my-dispatcher").withRouter(RoundRobinRouter(nrOfInstances = 20)))

  lazy val backupPartHandler = {
    val out: BackupPartHandler = actorOf[ZipBackupPartHandler]
    out.setup(this)
    out
  }
  lazy val hashListHandler = {
    val out: HashListHandler = actorOf[ZipHashListHandler]
    out.setup(this)
    out
  }
  lazy val cpuTaskHandler = new FutureCpuTaskHandler(this)
  lazy val blockHandler = {
    val out: BlockHandler = actorOf[ZipBlockHandler]
    out.setup(this)
    out
  }
}
