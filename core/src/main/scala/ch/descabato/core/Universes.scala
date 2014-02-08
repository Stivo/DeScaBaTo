package ch.descabato.core

import java.security.MessageDigest
import ch.descabato.CompressionMode
import ch.descabato.utils.CompressedStream
import ch.descabato.utils.Utils
import akka.actor._
import scala.concurrent.future
import scala.concurrent.Future
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.reflect.ClassTag
import scala.concurrent.ExecutionContext
import akka.routing.RoundRobinRouter
import akka.event.Logging
import akka.routing.DefaultResizer
import akka.routing.RouteeProvider
import java.util.concurrent.atomic.AtomicInteger
import akka.actor.TypedActor.PostRestart
import ch.descabato.utils.ZipFileHandlerFactory

object Universes {

}

trait UniversePart extends AnyRef with PostRestart {
  protected var universe: Universe = null
  protected def fileManager = universe.fileManager
  protected def config = universe.config
  def setup(universe: Universe) {
    this.universe = universe
    setupInternal()
    Thread.currentThread().setName(this.getClass.getSimpleName)
  }

  def postRestart(reason: Throwable) {
    System.exit(17)
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
  lazy val hashHandler = {
    val out = new SingleThreadHasher()
    out.setup(this)
    out
  }

  def finish = true

}

class SingleThreadCpuTaskHandler(universe: Universe) extends CpuTaskHandler {

  def computeHash(content: Array[Byte], hashMethod: String, blockId: BlockId) {
    val md = MessageDigest.getInstance(hashMethod)
    universe.backupPartHandler.hashComputed(blockId, md.digest(content), content)
  }

  def compress(hash: Array[Byte], content: Array[Byte], method: CompressionMode, disable: Boolean) {
    val (header, compressed) = CompressedStream.compress(content, method, disable)
    val name = Utils.encodeBase64Url(hash)
    val entry = ZipFileHandlerFactory.createZipEntry(name, header, compressed)
    universe.blockHandler.writeCompressedBlock(hash, entry, header, compressed)
  }

  def finish = true

}

class SingleThreadHasher extends HashHandler with UniversePart {
  lazy val md = universe.config.getMessageDigest
  def hash(blockId: BlockId, block: Array[Byte]) {
    md.update(block)
  }
  def finish(fd: FileDescription) {
    val hash = md.digest()
    universe.backupPartHandler.hashForFile(fd, hash)
  }

}

class AkkaCpuTaskHandler(universe: AkkaUniverse) extends SingleThreadCpuTaskHandler(universe) {

  override def computeHash(content: Array[Byte], hashMethod: String, blockId: BlockId) {
    universe.computer1 ! ((() => {
      super.computeHash(content, hashMethod, blockId)
    }): Function0[Unit])
  }

  override def compress(hash: Array[Byte], content: Array[Byte], method: CompressionMode, disable: Boolean) {
    universe.computer2 ! ((() => {
      super.compress(hash, content, method, disable)
    }))
  }

  override def finish() = {
    while (ActorStats.remaining.get() > 0) {
      Thread.sleep(100)
    }
    true
  }

}

object ActorStats {
  val remaining = new AtomicInteger(0)
}

class MyActor extends Actor {
  val log = Logging(context.system, this)
  var idleSince = System.currentTimeMillis()
  def receive = {
    case (x: Function0[_]) => {
      ActorStats.remaining.incrementAndGet()
      x()
      idleSince = System.currentTimeMillis()
      ActorStats.remaining.decrementAndGet()
    }
    case "idleSince" => sender ! idleSince
    case "test" => log.info("received test")
    case _ => log.info("received unknown message")
  }
}

class AkkaUniverse(val config: BackupFolderConfiguration) extends Universe with Utils {
  
  var system = ActorSystem("HelloSystem")

  val dispatcher = "backup-dispatcher"

  val taskers = 2

  val computer1 = system.actorOf(Props[MyActor]
    .withDispatcher(dispatcher).withRouter(RoundRobinRouter(nrOfInstances = taskers)))
  val computer2 = system.actorOf(Props[MyActor].withDispatcher("compressor-dispatcher").withRouter(RoundRobinRouter(20)))

  def actorOf[I, T <: I with UniversePart: ClassTag] = {
    val out: I = TypedActor(system).typedActorOf(p(TypedProps.apply[T]))
    out.asInstanceOf[UniversePart].setup(this)
    out
  }

  def p[T <: AnyRef](props: TypedProps[T]) = props.withDispatcher(dispatcher).withTimeout(1.day)

  //  val computer = system.actorOf(Props[AkkaCpuTaskHandler].withMailbox("ComputerMailbox")
  //      .withDispatcher("my-dispatcher").withRouter(RoundRobinRouter(nrOfInstances = 20)))
  lazy val backupPartHandler = actorOf[BackupPartHandler, ZipBackupPartHandler]
  lazy val hashListHandler = actorOf[HashListHandler, ZipHashListHandler]
  lazy val cpuTaskHandler = new AkkaCpuTaskHandler(this)
  lazy val blockHandler = actorOf[BlockHandler, ZipBlockHandler]
  lazy val hashHandler = actorOf[HashHandler, SingleThreadHasher]

  def finish() = {
    while (backupPartHandler.remaining > 0) {
      l.info(s"Waiting for backup to finish, ${backupPartHandler.remaining} files left")
      Thread.sleep(100)
    }
    while (blockHandler.remaining != 0) {
      l.info(s"Waiting for backup to finish, ${blockHandler.remaining} blocks left")
      Thread.sleep(100)
    }
    while (ActorStats.remaining.get() != 0) {
      l.info(s"Waiting for backup to finish, ${ActorStats.remaining} cpu tasks left")
      Thread.sleep(100)
    }
    cpuTaskHandler.finish
    while (blockHandler.remaining != 0) {
      l.info(s"Waiting for backup to finish, ${blockHandler.remaining} blocks left")
      Thread.sleep(100)
    }
    blockHandler.finish
    hashListHandler.finish
    backupPartHandler.finish
    true
  }
}

trait UnhandledExceptionLogging extends Utils {
  self: Actor with ActorLogging =>

  override def preRestart(reason:Throwable, message:Option[Any]){
    l.error("Unhandled exception for message: "+message, reason)
    System.exit(-17)
  }
}

class Resizer extends DefaultResizer(messagesPerResize = 100) with Utils {
  override def resize(routeeProvider: RouteeProvider): Unit = {
    val currentRoutees = routeeProvider.routees
    val requestedCapacity = capacity(currentRoutees)
    if (requestedCapacity != 0) {
      l.info("Changing number of compressor threads to " + (currentRoutees.size + requestedCapacity))
    }
    if (requestedCapacity > 0) routeeProvider.createRoutees(requestedCapacity)
    else if (requestedCapacity < 0) routeeProvider.removeRoutees(-requestedCapacity, stopDelay)
  }

}