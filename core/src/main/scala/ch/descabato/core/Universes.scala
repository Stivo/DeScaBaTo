package ch.descabato.core

import akka.actor.TypedActor.PostRestart
import ch.descabato.TimingUtil
import ch.descabato.akka.AkkaUniverse
import ch.descabato.core.storage.KvStoreBackupPartHandler
import ch.descabato.core.storage.KvStoreBlockHandler
import ch.descabato.core.storage.KvStoreHashListHandler
import ch.descabato.utils.CompressedStream
import ch.descabato.utils.Utils

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

class SingleThreadUniverse(val config: BackupFolderConfiguration) extends Universe with PureLifeCycle with Utils {
  def make[T <: UniversePart](x: T) = {x.setup(this); x}
  val journalHandler = make(new SimpleJournalHandler())
  val backupPartHandler = make(new KvStoreBackupPartHandler())
  val hashListHandler = make(new KvStoreHashListHandler())
  val cpuTaskHandler = new SingleThreadCpuTaskHandler(this)
  val blockHandler = make(new KvStoreBlockHandler())
  val hashHandler = make(new SingleThreadHasher())
  val compressionDecider = make(config.compressor match {
    case x if x.isCompressionAlgorithm => new SimpleCompressionDecider()
    case smart => new SmartCompressionDecider()
  })
  lazy val eventBus = new SimpleEventBus[BackupEvent]()

  load()

  override def finish() = {
    compressionDecider.report()
    finishOrder.map (_.finish).reduce(_ && _)
  }

  override def shutdown() = {
    journalHandler.finish()
    shutdownOrder.foreach { h =>
      try {
        h.shutdown()
      } catch {
        case e: Exception => 
          logger.error("exception while shutting down")
          logException(e)
      }
    }
    ret
  }
}

class SingleThreadCpuTaskHandler(universe: Universe) extends CpuTaskHandler {
  
  def computeHash(blockWrapper: Block) {
    val md = universe.config.getMessageDigest
    blockWrapper.hash = md.digest(blockWrapper.content)
    universe.backupPartHandler.hashComputed(blockWrapper)
  }

  def compress(block: Block) {
    val startAt = TimingUtil.getCpuTime
    val compressed = CompressedStream.compress(block.content, block.mode)
	  block.compressed = compressed
    val duration = TimingUtil.getCpuTime - startAt
    universe.compressionDecider.blockCompressed(block, duration)
  }

}

class SingleThreadHasher extends HashHandler with UniversePart with PureLifeCycle {
  lazy val md = universe.config.getMessageDigest

  def hash(block: Block) {
    md.update(block.content)
  }

  def finish(fd: FileDescription) {
    val hash = md.digest()
    universe.backupPartHandler.hashForFile(fd, hash)
  }

  def waitUntilQueueIsDone = true

  def fileFailed(fd: FileDescription) {
    md.reset()
  }

}
