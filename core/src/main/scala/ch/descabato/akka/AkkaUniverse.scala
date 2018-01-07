package ch.descabato.akka

import java.io.File
import java.lang.reflect.UndeclaredThrowableException
import java.util.concurrent._
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

import akka.actor._
import akka.dispatch.{DispatcherPrerequisites, ExecutorServiceConfigurator, ExecutorServiceFactory, MessageDispatcher}
import akka.event.Logging
import akka.pattern.AskTimeoutException
import akka.routing.{DefaultResizer, RoundRobinPool, Routee}
import ch.descabato.core.{BackupFolderConfiguration, _}
import ch.descabato.core.storage.{KvStoreBackupPartHandler, KvStoreBlockHandler, KvStoreHashListHandler}
import ch.descabato.frontend.{MaxValueCounter, ProgressReporters}
import ch.descabato.remote.{NoOpRemoteHandler, SimpleRemoteHandler}
import ch.descabato.utils.{BytesWrapper, Hash, Utils}
import ch.descabato.utils.Implicits._
import com.google.common.util.concurrent.RateLimiter
import com.typesafe.config.Config

import scala.collection.immutable.HashMap
import scala.collection.{immutable, mutable}
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}
import scala.util.matching.Regex

object Counter {
  var i = 0
}

class AkkaUniverse(val config: BackupFolderConfiguration) extends Universe with Utils {
  implicit val context: ExecutionContextExecutor = ExecutionContext.fromExecutor(ActorStats.tpe)

  val reg: Regex = "[^a-zA-Z0-9-_.*$+:@&=,!~';.]".r

  val cpuTaskCounter = new AtomicInteger()
  
  var system = ActorSystem("Descabato-"+Counter.i)

  system.whenTerminated.foreach(_ => this.terminated = true)(ExecutionContext.global)
  Counter.i += 1

  val queueLimit = 200

  val dispatcher = "backup-dispatcher"

  ActorStats.tpe.setCorePoolSize(Math.max(config.threads-1, 1))
  
  val taskers = 1

  val counters: mutable.Buffer[QueueCounter] = mutable.Buffer[QueueCounter]()

  lazy val hashers: ActorRef = system.actorOf(Props(classOf[MyActor], this)
    .withDispatcher(dispatcher).withRouter(RoundRobinPool(nrOfInstances = taskers).withResizer(new Resizer("hasher"))))

  val dispatch: MessageDispatcher = system.dispatchers.lookup(dispatcher)

  var terminated = false

  def actorOf[I <: AnyRef : Manifest, T <: I : Manifest](name: String, withCounter: Boolean = true, dispatcher: String = dispatcher): I = {
    val classI = manifest[I].runtimeClass.asInstanceOf[Class[I]]
    val classT = manifest[T].runtimeClass.asInstanceOf[Class[T]]
    val ref = TypedActor(system).typedActorOf(addDispatcher(
      TypedProps.apply[T](classI, classT), dispatcher = dispatcher), name = cleanUpName(name))
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

  def addDispatcher[T <: AnyRef](props: TypedProps[T], dispatcher: String = dispatcher): TypedProps[T] = props.withDispatcher(dispatcher).withTimeout(1.day)

  class QueueCounter(queueName: String, ref: AnyRef) extends MaxValueCounter {
    val name = s"Queue $queueName"
    maxValue = queueLimit * 2

    override def update() {
      queueInfo(ref) match {
        case Some((cur, max)) => current = cur; maxValue = max
        case _ =>
      }
    }
  }

  class CompressionTasksQueueCounter(name: String, getValue: => Long) extends QueueCounter(name, null) {
    maxValue = queueLimit * 4

    override def update() {
      try {
        current = getValue
      } catch {
        case x: UndeclaredThrowableException
          if x.getCause != null && x.getCause.isInstanceOf[AskTimeoutException] => // ignore
      }
    }
  }

  counters += new CompressionTasksQueueCounter("CPU Tasks", futureCounter.get())

  val journalHandler: JournalHandler = actorOf[JournalHandler, SimpleJournalHandler]("Journal Writer", dispatcher = "single-dispatcher")
  journalHandler.cleanUnfinishedFiles()
  val backupPartHandler: BackupPartHandler = actorOf[BackupPartHandler, KvStoreBackupPartHandler]("Backup Parts")
  val hashListHandler: HashListHandler = actorOf[HashListHandler, KvStoreHashListHandler]("Hash Lists", dispatcher = "backup-dispatcher")
  val blockHandler: BlockHandler = actorOf[BlockHandler, KvStoreBlockHandler]("Writer")
  val remoteHandler: RemoteHandler = {
    if (config.remoteOptions.enabled) {
      actorOf[RemoteHandler, SimpleRemoteHandler]("Remote")
    } else {
      actorOf[RemoteHandler, NoOpRemoteHandler]("Remote")
    }
  }
  lazy val hashFileHandler: HashFileHandler = actorOf[HashFileHandler, AkkaHasher]("Hasher")
  lazy val compressionDecider: CompressionDecider = config.compressor match {
    case x if x.isCompressionAlgorithm => actorOf[CompressionDecider, SimpleCompressionDecider]("Compression Decider")
    case _ => actorOf[CompressionDecider, SmartCompressionDecider]("Compression Decider")
  }

  def load() {
    startUpOrder.foreach { a =>
      if (a.mayUseNonBlockingLoad)
    	  a.load
      else
     	  a.loadBlocking
    }
  }

  val futureCounter = new AtomicLong(0)

  def scheduleTask[T](f: () => T): Future[T] = {
    futureCounter.getAndIncrement
    Future {
      val ret = f()
      futureCounter.getAndDecrement
      ret
    }
  }



  override def finish(): Boolean = {
    // In the end, more threads are needed because there is some synchronous operations
    val threads = ActorStats.tpe.getCorePoolSize()
    if (threads < 5) {
      ActorStats.tpe.setCorePoolSize(Math.max(threads, 5))
    }

    var toBeFinished = finishOrder
    while (toBeFinished.nonEmpty) {
      val head = toBeFinished.head
      // finish everything but the remote handler first, as usually that will be the last one to go
      // but if we wait to finish the other handlers before the remote handler they don't finish their
      // last file
      val waitForRemoteToo = head == remoteHandler
      waitForEmptyQueues(waitForRemoteToo)
      // finish first actor to be finished, it might add to the queues of other actors
      head.finish()
      logger.info("Finalized " + head.getClass)
      toBeFinished = toBeFinished.tail
    }
    //Thread.sleep(10000)
    true
  }

  val limiter = RateLimiter.create(0.016)

  private def waitForEmptyQueues(waitForRemote: Boolean): Unit = {
    var count = 0L
    do {
      if (count != 0) {
        Thread.sleep(100)
      }
      count = 0
      count += cpuTaskCounter.get
      count += futureCounter.get
      count += List(hashFileHandler, backupPartHandler, blockHandler, hashListHandler, journalHandler, compressionDecider)
        .map(queueLength).sum
      count += blockHandler.remaining + hashFileHandler.remaining() + backupPartHandler.remaining()
      if (waitForRemote) {
        count += remoteHandler.remaining()
      }
      if (limiter.tryAcquire(1)) {
        l.info(s"$count open items in all queues")
      }
    } while (count > 0)
  }

  def queueLength(x: AnyRef): Int = {
    val ref = TypedActor(system).getActorRefFor(x)
     Queues(Some(ref)) match {
       case Some(queue) => queue.queue.size()
       case None => l.info("Could not find queue for "+x); 0
     }
  }

  def queueInfo(x: AnyRef): Option[(Int, Int)] = {
    if (terminated)
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
      Thread.sleep(10)
    }
  }

  var prints = 0

  def shouldPrint = false //{prints += 1; prints % 10 == 0}

  def checkQueue(x: AnyRef, name: String, limit: Int = queueLimit) {
    var wait = true
    while (wait) {
      wait = queueInfo(x) match {
        case Some((cur, _)) if cur > limit =>
          Thread.sleep(50)
          if (shouldPrint)
            l.info(s"Waiting for queue $name with $cur queued items")
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
    checkQueueWithFunction(futureCounter.get().toInt, "Futures")
    checkQueueWithFunction(cpuTaskCounter.get(), "CPU Tasks")
    checkQueue(hashFileHandler, "hash handler")
    checkQueue(blockHandler, "writer")
    checkQueue(compressionDecider, "compression decider")
    //if (blockHandler.remaining > queueLimit*2)
    //l.info(s"Waiting for compressors to catch up ${blockHandler.remaining}")
//    while (blockHandler.remaining > queueLimit * 2) {
//      Thread.sleep(10)
//    }
    checkQueue(backupPartHandler, "backup parts")
  }

  override def shutdown(): BlockingOperation = {
    ActorStats.tpe.setCorePoolSize(10)
    super.shutdown()
    shutdownOrder.foreach(_.shutdown)
    shutdownOrder.collect{ case x: ActorRef => x}.foreach(_ ! PoisonPill)
    system.terminate()
    ret
  }

  override def createRestoreHandler(description: FileDescription, file: File, filecounter: MaxValueCounter): RestoreFileHandler = {
    if (description.size > 10 * config.blockSize.bytes) {
      val ref = actorOf[AkkaRestoreFileHandler, RestoreFileActor]("restore " + file.getAbsolutePath)
      ref.setup(description, file, ref, filecounter)
      ref
    } else {
      val ref = new SingleThreadRestoreFileHandler(description, file)
      ref.setup(this)
      ref
    }
  }

  def cleanUpName(name: String): String = {
    val simpleCleanup = name.replaceAll("\\s", "_").replace("\\", "__").replace("/", "__")
    reg.replaceAllIn(simpleCleanup, "")
  }
}

trait AkkaUniversePart extends UniversePart {
  def uni: AkkaUniverse = universe.asInstanceOf[AkkaUniverse]
  def add1: Int = uni.cpuTaskCounter.incrementAndGet()
  def subtract1: Int = uni.cpuTaskCounter.decrementAndGet()
}

object ActorStats {
  val tpe: ThreadPoolExecutor = {
    val out = new ThreadPoolExecutor(10, 10, 10, TimeUnit.MILLISECONDS,
      new LinkedBlockingQueue[Runnable]())
    out.setThreadFactory((r: Runnable) => {
      val t = Executors.defaultThreadFactory.newThread(r)
      t.setPriority(2)
      t.setDaemon(true)
      t
    })
    out
  }

}

class MyActor(override val uni: AkkaUniverse) extends Actor with AkkaUniversePart {
  val log = Logging(context.system, this)
  var idleSince: Long = System.currentTimeMillis()

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
  val counter: MaxValueCounter = new MaxValueCounter {
    override val name: String = Resizer.this.name+" threads"
    maxValue = upperBound
    ProgressReporters.addCounter(this)
  }
  override def resize(currentRoutees: immutable.IndexedSeq[Routee]): Int = {
    val maxThreads = ActorStats.tpe.getCorePoolSize
    if (maxThreads == currentRoutees.size) {
      counter.current = currentRoutees.size
      return 0
    }
    var requestedCapacity = capacity(currentRoutees)
    val newThreads = Math.min(requestedCapacity + currentRoutees.size, maxThreads)
    requestedCapacity = newThreads - currentRoutees.size
    requestedCapacity
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

class AkkaHasher extends HashFileHandler with UniversePart with PureLifeCycle with AkkaUniversePart {
  var map: Map[String, HashHandler] = new HashMap()

  def hash(block: Block) {
    add1
    val s = block.id.file.path
    if (map.get(s).isEmpty) {
      val actor = uni.actorOf[HashHandler, AkkaHashActor]("hasher for " + s, withCounter = false)
      map += s -> actor
    }
    map(s).hash(block.content)
  }

  def finish(fd: FileDescription) {
    add1
    val ref = map(fd.path)
    ref.finish { hash =>
      universe.backupPartHandler().hashForFile(fd, hash)
    }
    TypedActor.get(uni.system).getActorRefFor(ref) ! PoisonPill
    map -= fd.path
  }

  def fileFailed(fd: FileDescription) {
    map.get(fd.path) match {
      case Some(ref) =>
        TypedActor.get(uni.system).getActorRefFor(ref) ! PoisonPill
      case None =>
    }
    map -= fd.path
  }

  override def remaining(): Int = map.size
}

class AkkaHashActor extends SingleThreadHasher with AkkaUniversePart {
  override def finish(f: Hash => Unit): Unit = {
    super.finish(f)
    subtract1
  }

  override def hash(bytes: BytesWrapper) {
    super.hash(bytes)
    subtract1
  }
}
