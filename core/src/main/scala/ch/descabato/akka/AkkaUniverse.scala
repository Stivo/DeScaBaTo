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
import java.security.MessageDigest
import scala.collection.immutable.HashMap

object Counter {
  var i = 0
}

class AkkaUniverse(val config: BackupFolderConfiguration) extends Universe with Utils {

  var system = ActorSystem("Descabato-"+Counter.i)
  Counter.i += 1

  val queueLimit = 100

  val dispatcher = "backup-dispatcher"

  ActorStats.tpe.setCorePoolSize(Math.max(config.threads-1, 1))
  
  val taskers = 1

  val counters = mutable.Buffer[QueueCounter]()

  lazy val hashers = system.actorOf(Props[MyActor]
    .withDispatcher(dispatcher).withRouter(RoundRobinRouter(nrOfInstances = taskers).withResizer(new Resizer("hasher"))))
  lazy val compressors = system.actorOf(Props[MyActor].withDispatcher(dispatcher)
    .withRouter(RoundRobinRouter(4).withResizer(new Resizer("compressor"))))

  val dispatch = system.dispatchers.lookup(dispatcher)

  def actorOf[I <: AnyRef, T <: I : ClassTag](name: String, withCounter: Boolean = true, dispatcher: String = dispatcher) = {
    val ref = TypedActor(system).typedActorOf(p(TypedProps.apply[T], dispatcher = dispatcher))
    val out: I = ref
    out match {
      case u: UniversePart => u.setup(this)
      case _ =>
    }
    if (withCounter) {
      val q = new QueueCounter(name, out)
      counters += q
    }
    out
  }

  def p[T <: AnyRef](props: TypedProps[T], dispatcher: String = dispatcher) = props.withDispatcher(dispatcher).withTimeout(1.day)

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

  class CompressionTasksQueueCounter(name: String, getValue: => Long) extends QueueCounter(name, compressors) {
    maxValue = queueLimit * 4

    override def update {
      try {
        current = getValue
      } catch {
        case x: UndeclaredThrowableException
          if x.getCause != null && x.getCause.isInstanceOf[AskTimeoutException] => // ignore
      }
    }
  }

  counters += new CompressionTasksQueueCounter("Compression", blockHandler.remaining)
  counters += new CompressionTasksQueueCounter("CPU Tasks", ActorStats.remaining.get)

  val journalHandler = actorOf[JournalHandler, SimpleJournalHandler]("Journal Writer", dispatcher = "single-dispatcher")
  journalHandler.usedIdentifiers()
  val backupPartHandler = actorOf[BackupPartHandler, ZipBackupPartHandler]("Backup Parts")
  val hashListHandler = actorOf[HashListHandler, NewZipHashListHandler]("Hash Lists", dispatcher = "single-dispatcher")
  lazy val eventBus = actorOf[EventBus[BackupEvent], SimpleEventBus[BackupEvent]]("Event Bus")
  lazy val cpuTaskHandler = new AkkaCpuTaskHandler(this)
  val blockHandler = actorOf[BlockHandler, ZipBlockHandler]("Writer", dispatcher = "single-dispatcher")
  lazy val hashHandler = actorOf[HashHandler, AkkaHasher]("Hasher")
  // TODO compressionStatistics
  //override lazy val compressionStatistics = Some(actorOf[CompressionStatistics, SimpleCompressionStatistics]("Statistics", false))

  override def finish() = {
    // order to wait, from front to back
    checkQueueWithFunction(hashHandler.remaining(), "file hashers remaining", 0)
    checkQueueWithFunction(ActorStats.remaining.get(), "cpu task handlers", 0)
    checkQueue(hashHandler, "block hashers queue", 0)
    checkQueueWithFunction(backupPartHandler.remaining, "Backup parts", 0)
    checkQueueWithFunction(blockHandler.remaining, "blocks to compress", 0)
    checkQueue(hashListHandler, "Hash lists", 0)
    // Actually finishing the backup, from inside out
    blockHandler.finish
    hashListHandler.finish
    backupPartHandler.finish
    journalHandler.finish()
    true
  }

  def queueInfo(x: AnyRef): Option[(Int, Int)] = {
    if (system.isTerminated)
      return None
    try {
      val ref = TypedActor(system).getActorRefFor(x)
      val queue = Queues(Some(ref)).get.queue
      Some((queue.size(), queueLimit * 2))
    } catch {
      case e: Exception => logException(e)
        None
    }
  }

  var checks = 1

  def updateProgress() {
    ProgressReporters.updateWithCounters(counters)
  }

  def checkQueueWithFunction(f: => Int, name: String, limit: Int = queueLimit) {
    while (f > limit) {
      l.info(s"Waiting for $name to finish, has $f left")
      Thread.sleep(100)
    }
  }

  def checkQueue(x: AnyRef, name: String, limit: Int = queueLimit) {
    var wait = true
    while (wait) {
      wait = queueInfo(x) match {
        case Some((cur, _)) if cur > limit =>
          Thread.sleep(10);
          l.info(s"Waiting for queue ${name} with $cur queued items")
          true
        case _ => false
      }
    }
  }

  override def waitForQueues() {
    checks += 1
    updateProgress()
    if (checks % 20 != 0) {
      return
    }
    checkQueue(hashHandler, "hash handler")
    checkQueue(eventBus, "event bus", 0)
    //if (blockHandler.remaining > queueLimit*2)
    //l.info(s"Waiting for compressors to catch up ${blockHandler.remaining}")
    while (blockHandler.remaining > queueLimit * 2) {
      Thread.sleep(10)
    }
    checkQueue(backupPartHandler, "backup parts")
  }

  override def shutdown() = {
    super.shutdown()
    system.shutdown()
  }
}

class AkkaCpuTaskHandler(universe: AkkaUniverse) extends SingleThreadCpuTaskHandler(universe) {

  override def computeHash(content: Array[Byte], hashMethod: String, blockId: BlockId) {
    universe.hashers ! (() => {
      super.computeHash(content, hashMethod, blockId)
    })
  }

  override def compress(blockId: BlockId, hash: Array[Byte], content: Array[Byte], method: CompressionMode, disable: Boolean) {
    universe.compressors ! (() => {
      super.compress(blockId, hash, content, method, disable)
    })
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
//        println("Created new thread " + t.getName())
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
    case (x: Function0[_]) =>
      ActorStats.remaining.incrementAndGet()
      x()
      idleSince = System.currentTimeMillis()
      ActorStats.remaining.decrementAndGet()
    case "idleSince" => sender ! idleSince
    case "test" => log.info("received test")
    case _ => log.info("received unknown message")
  }
}


class Resizer(name: String) extends DefaultResizer(messagesPerResize = 100) with Utils {
  val counter = new MaxValueCounter {
    override val name: String = Resizer.this.name+" threads"
    maxValue = upperBound
    ProgressReporters.addCounter(this)
  }
  override def resize(routeeProvider: RouteeProvider): Unit = {
    val maxThreads = ActorStats.tpe.getCorePoolSize
    val currentRoutees = routeeProvider.routees
    if (maxThreads == currentRoutees.size) {
      counter.current = currentRoutees.size
      return
    }
    var requestedCapacity = capacity(currentRoutees)
    var endResult = requestedCapacity + currentRoutees.size
    val newThreads = Math.min(requestedCapacity + currentRoutees.size, maxThreads)
    requestedCapacity = newThreads - currentRoutees.size
//    if (requestedCapacity != 0) {
//      l.info(s"Changing number of $name threads to " + (currentRoutees.size + requestedCapacity))
//    }
    counter.current = currentRoutees.size + requestedCapacity
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

class AkkaHasher extends HashHandler with UniversePart {
  var map: Map[String, HashHandler] = new HashMap()

  def akkaUniverse = universe.asInstanceOf[AkkaUniverse]

  def hash(blockId: BlockId, block: Array[Byte]) {
    val s = blockId.file.path
    if (map.get(s).isEmpty) {
      map += s -> akkaUniverse.actorOf[HashHandler, SingleThreadHasher]("hasher for " + s, false)
    }
    map(s).hash(blockId, block)
  }

  def finish(fd: FileDescription) {
    val ref = map(fd.path)
    ref.finish(fd)
    TypedActor.get(akkaUniverse.system).getActorRefFor(ref) ! PoisonPill
    map -= fd.path
  }

  def fileFailed(fd: FileDescription) {
    val ref = map(fd.path)
    ref.fileFailed(fd)
    TypedActor.get(akkaUniverse.system).getActorRefFor(ref) ! PoisonPill
    map -= fd.path
  }

  override def remaining(): Int = map.size
}

class AkkaEventBus extends SimpleEventBus with UniversePart
