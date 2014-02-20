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
import ch.descabato.frontend.{Counter, ProgressReporters, MaxValueCounter}
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

  val cpuTaskCounter = new AtomicInteger()
  
  var system = ActorSystem("Descabato-"+Counter.i)
  Counter.i += 1

  val queueLimit = 50

  val dispatcher = "backup-dispatcher"

  ActorStats.tpe.setCorePoolSize(Math.max(config.threads-1, 1))
  
  val taskers = 1

  val counters = mutable.Buffer[QueueCounter]()

  lazy val hashers = system.actorOf(Props(classOf[MyActor], this)
    .withDispatcher(dispatcher).withRouter(RoundRobinRouter(nrOfInstances = taskers).withResizer(new Resizer("hasher"))))
  lazy val compressors = system.actorOf(Props(classOf[MyActor], this).withDispatcher(dispatcher)
    .withRouter(RoundRobinRouter(4).withResizer(new Resizer("compressor"))))

  val dispatch = system.dispatchers.lookup(dispatcher)

  def actorOf[I <: AnyRef : Manifest, T <: I : Manifest](name: String, withCounter: Boolean = true, dispatcher: String = dispatcher) = {
    val classI = manifest[I].runtimeClass.asInstanceOf[Class[I]]
    val classT = manifest[T].runtimeClass.asInstanceOf[Class[T]]
    val ref = TypedActor(system).typedActorOf(p(
      TypedProps.apply[T](classI, classT), dispatcher = dispatcher))
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

  class QueueCounter(queueName: String, ref: AnyRef) extends MaxValueCounter {
    val name = s"Queue $queueName"
    maxValue = 500

    override def update {
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

  counters += new CompressionTasksQueueCounter("CPU Tasks", cpuTaskCounter.get())

  val journalHandler = actorOf[JournalHandler, SimpleJournalHandler]("Journal Writer", dispatcher = "single-dispatcher")
  journalHandler.cleanUnfinishedFiles()
  val backupPartHandler = actorOf[BackupPartHandler, ZipBackupPartHandler]("Backup Parts")
  val hashListHandler = actorOf[HashListHandler, ZipHashListHandler]("Hash Lists", dispatcher = "backup-dispatcher")
  lazy val eventBus = actorOf[EventBus[BackupEvent], SimpleEventBus[BackupEvent]]("Event Bus")
  lazy val cpuTaskHandler = new AkkaCpuTaskHandler(this)
  val blockHandler = actorOf[BlockHandler, ZipBlockHandler]("Writer")
  lazy val hashHandler = actorOf[HashHandler, AkkaHasher]("Hasher")
  lazy val compressionDecider = config.compressor match {
    case x if x.isCompressionAlgorithm => actorOf[CompressionDecider, SimpleCompressionDecider]("Compression Decider")
    case smart => actorOf[CompressionDecider, SmartCompressionDecider]("Compression Decider")
  }

  def load() {
    startUpOrder.foreach { a =>
      if (a.mayUseNonBlockingLoad)
    	  a.load
      else
     	  a.loadBlocking
    }
    eventBus.subscribe(counterUpdater)
  }

  val counterUpdater: PartialFunction[BackupEvent, Unit] = {
    case Subtract1CpuTask => cpuTaskCounter.decrementAndGet()
    case Add1CpuTask => cpuTaskCounter.incrementAndGet()
  }
  
  override def finish() = {
    var count = 0
    do {
      if (count != 0) {
        Thread.sleep(100)
      }
      count = 0
      count += cpuTaskCounter.get
      count += List(hashHandler, backupPartHandler, blockHandler, hashListHandler, journalHandler, compressionDecider)
                .map(queueLength).sum
      count += blockHandler.remaining + hashHandler.remaining() + backupPartHandler.remaining()
      l.info(s"$count open items in all queues")
    } while (count > 0)
    // In the end, more threads are needed because there is some synchronous operations
    val threads = ActorStats.tpe.getCorePoolSize()
    ActorStats.tpe.setCorePoolSize(Math.max(threads, 3))
    //Thread.sleep(10000)
    // Actually finishing the backup, from inside out
    finishOrder.foreach { _.finish }
    true
  }

  def queueLength(x: AnyRef) = {
    val ref = TypedActor(system).getActorRefFor(x)
     Queues(Some(ref)) match {
       case Some(queue) => queue.queue.size()
       case None => l.info("Could not find queue for "+x); 0
     }
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

  def checkQueueWithFunction(f: => Int, name: String, limit: Int = queueLimit) {
    while (f > limit) {
      if (shouldPrint)
        l.info(s"Waiting for $name to finish, has $f left")
      Thread.sleep(50)
    }
  }

  var prints = 0

  def shouldPrint = false //{prints += 1; prints % 10 == 0}

  def checkQueue(x: AnyRef, name: String, limit: Int = queueLimit) {
    var wait = true
    while (wait) {
      wait = queueInfo(x) match {
        case Some((cur, _)) if cur > limit =>
          Thread.sleep(50);
          if (shouldPrint)
            l.info(s"Waiting for queue ${name} with $cur queued items")
          true
        case _ => false
      }
    }
  }

  override def waitForQueues() {
    checks += 1
    if (checks % 10 != 0) {
      return
    }
    ProgressReporters.addCounter(counters: _*)
    checkQueueWithFunction(cpuTaskCounter.get(), "CPU Tasks")
    checkQueue(hashHandler, "hash handler")
    checkQueue(eventBus, "event bus")
    checkQueue(blockHandler, "writer")
    checkQueue(compressionDecider, "compression decider")
    //if (blockHandler.remaining > queueLimit*2)
    //l.info(s"Waiting for compressors to catch up ${blockHandler.remaining}")
//    while (blockHandler.remaining > queueLimit * 2) {
//      Thread.sleep(10)
//    }
    checkQueue(backupPartHandler, "backup parts")
  }

  override def shutdown() = {
    ActorStats.tpe.setCorePoolSize(10)
    super.shutdown()
    shutdownOrder.foreach(_.shutdown)
    shutdownOrder.collect{ case x: ActorRef => x}.foreach(_ ! PoisonPill)
    system.shutdown()
    ret
  }
}

trait AkkaUniversePart extends UniversePart {
  def uni = universe.asInstanceOf[AkkaUniverse]
  def add1 = uni.cpuTaskCounter.incrementAndGet()
  def subtract1 = uni.cpuTaskCounter.decrementAndGet()
}

class AkkaCpuTaskHandler(universe: AkkaUniverse) extends SingleThreadCpuTaskHandler(universe) with AkkaUniversePart {

  setup(universe)

  override def computeHash(block: Block) {
    add1
    universe.hashers ! (() => {
      super.computeHash(block)
      subtract1
    })
  }

  override def compress(block: Block) {
    add1
    universe.compressors ! (() => {
      super.compress(block)
      subtract1
    })
  }

  override def finish() = {
    true
  }
}

object ActorStats {
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

class MyActor(override val uni: AkkaUniverse) extends Actor with AkkaUniversePart {
  val log = Logging(context.system, this)
  var idleSince = System.currentTimeMillis()

  def receive = {
    case (x: Function0[_]) =>
      x()
      idleSince = System.currentTimeMillis()
    case "idleSince" => sender ! idleSince
    case "test" => log.info("received test")
    case _ => log.info("received unknown message")
  }
  
  override def postRestart(reason: Throwable) {
    System.exit(17)
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

class AkkaHasher extends HashHandler with UniversePart with PureLifeCycle with AkkaUniversePart {
  var map: Map[String, HashHandler] = new HashMap()

  def hash(block: Block) {
    add1
    val s = block.id.file.path
    if (map.get(s).isEmpty) {
      val actor = uni.actorOf[HashHandler, AkkaSingleThreadHasher]("hasher for " + s, false)
      map += s -> actor
    }
    map(s).hash(block)
  }

  def finish(fd: FileDescription) {
    add1
    val ref = map(fd.path)
    ref.finish(fd)
    TypedActor.get(uni.system).getActorRefFor(ref) ! PoisonPill
    map -= fd.path
  }

  def fileFailed(fd: FileDescription) {
    map.get(fd.path) match {
      case Some(ref) =>
        ref.fileFailed(fd)
        TypedActor.get(uni.system).getActorRefFor(ref) ! PoisonPill
      case None =>
    }
    map -= fd.path
  }

  override def remaining(): Int = map.size
}

class AkkaEventBus extends SimpleEventBus with UniversePart

class AkkaSingleThreadHasher extends SingleThreadHasher with AkkaUniversePart {
  override def hash(block: Block) {
    super.hash(block)
    subtract1
  }

  override def finish(fd: FileDescription) {
    super.finish(fd)
    subtract1
  }
}
