package ch.descabato.core

import java.security.MessageDigest
import ch.descabato.{TimingUtil, CompressionMode}
import ch.descabato.utils.CompressedStream
import ch.descabato.utils.Utils
import akka.actor._
import scala.concurrent.future
import scala.concurrent.Future
import scala.concurrent.ExecutionContext
import akka.event.Logging
import akka.actor.TypedActor.PostRestart
import ch.descabato.utils.ZipFileHandlerFactory
import com.typesafe.config.Config
import akka.dispatch.DispatcherPrerequisites
import java.util.concurrent.ThreadFactory
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import ch.descabato.akka.AkkaUniverse
import scala.collection.mutable
import ch.descabato.frontend.MaxValueCounter

object Universes {
  def makeUniverse(config: BackupFolderConfiguration) = {
    if (config.threads == 1)
      new SingleThreadUniverse(config)
    else
      new AkkaUniverse(config)
  }
}

trait UniversePart extends AnyRef with PostRestart {
  protected def universe = _universe
  protected var _universe: Universe = null
  protected def fileManager = universe.fileManager

  protected def config = universe.config

  def setup(universe: Universe) {
    this._universe = universe
    setupInternal()
  }

  def postRestart(reason: Throwable) {
    System.exit(17)
  }

  protected def setupInternal() {}
}

class SingleThreadUniverse(val config: BackupFolderConfiguration) extends Universe with PureLifeCycle {
  def make[T <: UniversePart](x: T) = {x.setup(this); x}
  val journalHandler = make(new SimpleJournalHandler())
  val backupPartHandler = make(new ZipBackupPartHandler())
  val hashListHandler = make(new ZipHashListHandler())
  val cpuTaskHandler = new SingleThreadCpuTaskHandler(this)
  val blockHandler = make(new ZipBlockHandler())
  val hashHandler = make(new SingleThreadHasher())
  lazy val eventBus = new SimpleEventBus[BackupEvent]()

  load()

  override def finish() = {
    finishOrder.map (_.finish).reduce(_ && _)
  }

  override def shutdown() = {
    journalHandler.finish()
    shutdownOrder.foreach { _.shutdown() }
    ret
  }
}

class SingleThreadCpuTaskHandler(universe: Universe) extends CpuTaskHandler {

  def computeHash(content: Array[Byte], hashMethod: String, blockId: BlockId) {
    val md = MessageDigest.getInstance(hashMethod)
    universe.backupPartHandler.hashComputed(blockId, md.digest(content), content)
  }

  def compress(blockId: BlockId, hash: Array[Byte], content: Array[Byte], method: CompressionMode, disable: Boolean) {
    val startAt = TimingUtil.getCpuTime
    val (header, compressed) = CompressedStream.compress(content, method, disable)
    val duration = TimingUtil.getCpuTime - startAt
    val name = Utils.encodeBase64Url(hash)
    val entry = ZipFileHandlerFactory.createZipEntry("blocks/"+name, header, compressed)
    universe.compressionStatistics().foreach {
      _.blockStatistics(blockId, compressed.remaining(), content.size, method, duration)
    }
    universe.blockHandler.writeCompressedBlock(hash, entry, header, compressed)
  }

}

class SingleThreadHasher extends HashHandler with UniversePart with PureLifeCycle {
  lazy val md = universe.config.getMessageDigest

  def hash(blockId: BlockId, block: Array[Byte]) {
    md.update(block)
  }

  def finish(fd: FileDescription) {
    val hash = md.digest()
    universe.backupPartHandler.hashForFile(fd, hash)
  }

  def waitUntilQueueIsDone = true

  def fileFailed(fd: FileDescription) {}

}
