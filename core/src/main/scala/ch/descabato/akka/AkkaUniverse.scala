package ch.descabato.akka

import akka.routing.{RoundRobinRouter, RouteeProvider, DefaultResizer}
import ch.descabato.utils.Utils
import com.typesafe.config.Config
import akka.dispatch.{ExecutorServiceFactory, ExecutorServiceConfigurator, DispatcherPrerequisites}
import java.util.concurrent._
import scala.concurrent.duration._
import ch.descabato.core._
import akka.actor._
import scala.collection.mutable
import scala.reflect.ClassTag
import ch.descabato.frontend.{ProgressReporters, UpdatingCounter, MaxValueCounter}
import java.util.concurrent.atomic.AtomicInteger
import akka.event.Logging
import ch.descabato.CompressionMode
import ch.descabato.core.BlockId
import ch.descabato.core.BackupFolderConfiguration
import akka.pattern.AskTimeoutException
import java.lang.reflect.UndeclaredThrowableException

class AkkaUniverse(val config: BackupFolderConfiguration) extends Universe with Utils {

  var system = ActorSystem("HelloSystem")

  val queueLimit = 20

  val dispatcher = "backup-dispatcher"

  val taskers = 1

  val counters = mutable.Buffer[QueueCounter]()

  val computer1 = system.actorOf(Props[MyActor]
    .withDispatcher(dispatcher).withRouter(RoundRobinRouter(nrOfInstances = taskers).withResizer(new Resizer("hasher"))))
  val computer2 = system.actorOf(Props[MyActor].withDispatcher(dispatcher)
    .withRouter(RoundRobinRouter(4).withResizer(new Resizer("compressor"))))

  val dispatch = system.dispatchers.lookup(dispatcher)

  def actorOf[I <: AnyRef, T <: I with UniversePart: ClassTag](name: String) = {
    val ref = TypedActor(system).typedActorOf(p(TypedProps.apply[T]))
    val out: I = ref
    out.asInstanceOf[UniversePart].setup(this)
    val q = new QueueCounter(name, out)
    counters += q
    out
  }

  def p[T <: AnyRef](props: TypedProps[T]) = props.withDispatcher(dispatcher).withTimeout(1.day)

  class QueueCounter(queueName: String, ref: AnyRef) extends MaxValueCounter with UpdatingCounter {
    val name = s"Queue $queueName"
    maxValue = 500
    def update {
      queueInfo(ref) match {
        case Some((cur, max)) => current = cur; maxValue = max
        case _ =>
      }
    }
  }

  class CompressionTasksQueueCounter extends QueueCounter("Compression", computer2) {
    maxValue = queueLimit*4
    override def update {
      try {
        current = blockHandler.remaining
      } catch {
        case x: UndeclaredThrowableException
          if x.getCause != null && x.getCause.isInstanceOf[AskTimeoutException] => // ignore
      }
    }
  }

  counters += new CompressionTasksQueueCounter

  lazy val backupPartHandler = actorOf[BackupPartHandler, ZipBackupPartHandler]("Backup Parts")
  lazy val hashListHandler = actorOf[HashListHandler, ZipHashListHandler]("Hash Lists")
  lazy val cpuTaskHandler = new AkkaCpuTaskHandler(this)
  lazy val blockHandler = actorOf[BlockHandler, ZipBlockHandler]("Writer")
  lazy val hashHandler = actorOf[HashHandler, SingleThreadHasher]("Hasher")

  override def finish() {
    while (backupPartHandler.remaining > 0) {
      l.info(s"Waiting for backup to finish, ${backupPartHandler.remaining} files left")
      updateProgress()
      Thread.sleep(100)
    }
    while (blockHandler.remaining != 0) {
      l.info(s"Waiting for backup to finish, ${blockHandler.remaining} blocks left")
      updateProgress()
      Thread.sleep(100)
    }
    while (ActorStats.remaining.get() != 0) {
      l.info(s"Waiting for backup to finish, ${ActorStats.remaining} cpu tasks left")
      updateProgress()
      Thread.sleep(100)
    }
    cpuTaskHandler.finish
    while (blockHandler.remaining != 0) {
      updateProgress()
      l.info(s"Waiting for backup to finish, ${blockHandler.remaining} blocks left")
      Thread.sleep(100)
    }
    blockHandler.finish
    hashListHandler.finish
    backupPartHandler.finish
  }

  def queueInfo(x: AnyRef): Option[(Int, Int)] = {
    if (system.isTerminated)
      return None
    try {
      val ref = TypedActor(system).getActorRefFor(x)
      val queue = Queues(Some(ref)).get.queue
      Some((queue.size(), queueLimit*2))
    } catch {
      case e: Exception => logException(e)
        None
    }
  }

  var checks = 1

  def updateProgress() {
    ProgressReporters.updateWithCounters(counters)
  }

  override def waitForQueues() {
    checks += 1
    updateProgress()
    if (checks % 20 != 0) {
      return
    }
    def checkQueue(x: AnyRef, name: String) {
      var wait = true
      while (wait) {
        wait = queueInfo(x) match {
          case Some((cur, _)) if cur > queueLimit =>
            Thread.sleep(10);
            //l.info(s"Waiting for queue ${name} with $cur queued items")
            true
          case _ => false
        }
      }
    }
    checkQueue(hashHandler, "hash handler")
    //if (blockHandler.remaining > queueLimit*2)
      //l.info(s"Waiting for compressors to catch up ${blockHandler.remaining}")
    while (blockHandler.remaining > queueLimit*2) {
      Thread.sleep(10)
    }
    checkQueue(backupPartHandler, "backup parts")
  }

  override def shutdown() = system.shutdown()
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
  val tpe = {
    val out = new ThreadPoolExecutor(10, 10, 10, TimeUnit.MILLISECONDS,
      new LinkedBlockingQueue[Runnable]())
    out.setThreadFactory(new ThreadFactory() {
      def newThread(r: Runnable) = {
        val t = Executors.defaultThreadFactory.newThread(r)
        t.setPriority(2)
        t.setDaemon(true)
        println("Created new thread " + t.getName())
        t
      }
    })
    out
  }

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


class Resizer(name: String) extends DefaultResizer(messagesPerResize = 100) with Utils {
  override def resize(routeeProvider: RouteeProvider): Unit = {
    val maxThreads = ActorStats.tpe.getCorePoolSize
    val currentRoutees = routeeProvider.routees
    if (maxThreads == currentRoutees.size)
      return
    var requestedCapacity = capacity(currentRoutees)
    var endResult = requestedCapacity + currentRoutees.size
    val newThreads = Math.min(requestedCapacity+currentRoutees.size, maxThreads)
    requestedCapacity = newThreads - currentRoutees.size
    if (requestedCapacity != 0) {
      l.info(s"Changing number of $name threads to " + (currentRoutees.size + requestedCapacity))
    }
    if (requestedCapacity > 0) routeeProvider.createRoutees(requestedCapacity)
    else if (requestedCapacity < 0) routeeProvider.removeRoutees(-requestedCapacity, stopDelay)
  }

}


class MyExecutorServiceConfigurator(config: Config, prerequisites: DispatcherPrerequisites) extends ExecutorServiceConfigurator(config, prerequisites) {
  def createExecutorServiceFactory(id: String, threadFactory: ThreadFactory) = new MyExecutorServiceFactory
}

class MyExecutorServiceFactory extends ExecutorServiceFactory {
  def createExecutorService: ExecutorService = {
    ActorStats.tpe
  }
}

