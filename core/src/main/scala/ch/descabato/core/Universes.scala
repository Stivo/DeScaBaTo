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
import akka.dispatch.ExecutorServiceConfigurator
import com.typesafe.config.Config
import akka.dispatch.DispatcherPrerequisites
import java.util.concurrent.ExecutorService
import akka.dispatch.ExecutorServiceFactory
import java.util.concurrent.ThreadFactory
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.Executors
import ch.descabato.akka.{AkkaUniverse, Queues}
import scala.collection.mutable
import ch.descabato.frontend.{UpdatingCounter, ProgressReporters, MaxValueCounter}

object Universes {
  def makeUniverse(config: BackupFolderConfiguration) = {
    new AkkaUniverse(config)
  }
}

trait UniversePart extends AnyRef with PostRestart {
  protected var universe: Universe = null

  protected def fileManager = universe.fileManager

  protected def config = universe.config

  def setup(universe: Universe) {
    this.universe = universe
    setupInternal()
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

  def waitUntilQueueIsDone = true

}
