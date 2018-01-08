package ch.descabato.core

import java.io.{File, FileOutputStream, OutputStream}
import java.security.MessageDigest

import akka.actor.TypedActor.PostRestart
import ch.descabato.CompressionMode
import ch.descabato.akka.{ActorStats, AkkaUniverse}
import ch.descabato.core.storage.{KvStoreBackupPartHandler, KvStoreBlockHandler, KvStoreHashListHandler}
import ch.descabato.frontend.MaxValueCounter
import ch.descabato.remote.{NoOpRemoteHandler, SingleThreadRemoteHandler}
import ch.descabato.utils.Implicits._
import ch.descabato.utils._
import org.apache.commons.compress.utils.IOUtils

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}

object Universes {
  def makeUniverse(config: BackupFolderConfiguration): Universe with Utils = {
    if (config.threads == 1)
      new SingleThreadUniverse(config)
    else
      new AkkaUniverse(config)
  }
}

trait UniversePart extends AnyRef with PostRestart {
  protected def universe: Universe = _universe
  protected var _universe: Universe = _
  protected def fileManager: FileManager = universe.fileManager
  protected implicit val executionContext: ExecutionContextExecutor = ExecutionContext.fromExecutor(ActorStats.tpe)

  protected def config: BackupFolderConfiguration = universe.config

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
  def make[T <: UniversePart](x: T): T = {x.setup(this); x}
  val journalHandler: SimpleJournalHandler = make(new SimpleJournalHandler())
  val backupPartHandler: KvStoreBackupPartHandler = make(new KvStoreBackupPartHandler())
  val hashListHandler: KvStoreHashListHandler = make(new KvStoreHashListHandler())
  val blockHandler: KvStoreBlockHandler = make(new KvStoreBlockHandler())
  val hashFileHandler: SingleThreadFileHasher = make(new SingleThreadFileHasher())
  val compressionDecider: CompressionDecider = make(config.compressor match {
    case x if x.isCompressionAlgorithm => new SimpleCompressionDecider()
    case CompressionMode.smart => new SmartCompressionDecider()
    case _ => new SimpleCompressionDecider(Some(CompressionMode.lz4hc))
  })
  val remoteHandler: RemoteHandler = make(if (config.remoteOptions.enabled) {
    new SingleThreadRemoteHandler()
  } else {
    new NoOpRemoteHandler()
  })
  load()

  override def finish(): Boolean = {
    compressionDecider.report()
    finishOrder.map (_.finish).reduce(_ && _)
  }

  override def shutdown(): BlockingOperation = {
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

  override def scheduleTask[T](f: () => T): Future[T] = {
    Future.successful(f())
  }

  override def createRestoreHandler(description: FileDescription, file: File, counter: MaxValueCounter): RestoreFileHandler = {
    val out = new SingleThreadRestoreFileHandler(description, file)
    out.setup(this)
    out
  }
}

class SingleThreadHasher extends HashHandler {
  lazy val md: MessageDigest = universe.config.createMessageDigest

  def finish(f: Hash => Unit) {
    f(new Hash(md.digest()))
    md.reset()
  }

  override def hash(bytes: BytesWrapper) {
    md.update(bytes)
  }
}

class SingleThreadRestoreFileHandler(val fd: FileDescription, val destination: File) extends RestoreFileHandler {

  var hashList: Array[Array[Byte]] = _

  val hasher = new SingleThreadHasher()

  override def restore(): Future[Boolean] = {
    hasher.setup(universe)
    val os: OutputStream = null
    try {
      val hashList = if (fd.hasHashList) {
        universe.hashListHandler().getHashlist(fd.hash, fd.size).toArray
      } else {
        Array.apply(fd.hash)
      }
      destination.getParentFile.mkdirs()
      val os = new FileOutputStream(destination)
      for (hash <- hashList) {
        val in = universe.blockHandler().readBlock(hash)
        val decomp = CompressedStream.decompressToBytes(in)
        hasher.hash(decomp)
        os.write(decomp)
      }
      os.close()
      var success = true
      hasher.finish { hash =>
        success = hash === fd.hash
      }
      Future.successful(success)
    } catch {
      case e: Exception =>
        Future.failed(e)
    } finally {
      IOUtils.closeQuietly(os)
    }
  }
}

class SingleThreadFileHasher extends HashFileHandler with PureLifeCycle {
  val hasher = new SingleThreadHasher()

  override def setup(universe: Universe): Unit = {
    super.setup(universe)
    hasher.setup(universe)
  }
  // Hash this block of this file
  override def hash(blockwrapper: Block) {
    hasher.hash(blockwrapper.content)
  }

  override def finish(fd: FileDescription): Unit = {
    hasher.finish { hash =>
      universe.backupPartHandler().hashForFile(fd, hash)
    }
  }

  override def fileFailed(fd: FileDescription): Unit = {
    hasher.finish { _ => }
  }
}