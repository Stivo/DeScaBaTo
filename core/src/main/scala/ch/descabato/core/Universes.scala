package ch.descabato.core

import java.io.{OutputStream, FileOutputStream, File}
import java.security.DigestOutputStream

import akka.actor.TypedActor.PostRestart
import ch.descabato.akka.{ActorStats, AkkaUniverse}
import ch.descabato.core.storage.{KvStoreBackupPartHandler, KvStoreBlockHandler, KvStoreHashListHandler}
import ch.descabato.utils.{Hash, Streams, CompressedStream, Utils}
import org.apache.commons.compress.utils.IOUtils

import scala.concurrent.{ExecutionContext, Future}

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
  protected implicit val executionContext = ExecutionContext.fromExecutor(ActorStats.tpe)

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
  val blockHandler = make(new KvStoreBlockHandler())
  val hashFileHandler = make(new SingleThreadFileHasher())
  val compressionDecider = make(config.compressor match {
    case x if x.isCompressionAlgorithm => new SimpleCompressionDecider()
    case smart => new SmartCompressionDecider()
  })

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

  override def scheduleTask[T](f: () => T): Future[T] = {
    Future.successful(f())
  }

  override def createRestoreHandler(description: FileDescription, file: File): RestoreFileHandler = {
    val out = new SingleThreadRestoreFileHandler(description, file)
    out.setup(this)
    out
  }
}

class SingleThreadHasher extends HashHandler {
  lazy val md = universe.config.getMessageDigest

  def finish(f: Hash => Unit) {
    f(new Hash(md.digest()))
    md.reset()
  }

  override def hash(bytes: Array[Byte]) {
    md.update(bytes)
  }
}

class SingleThreadRestoreFileHandler(val fd: FileDescription, val destination: File) extends RestoreFileHandler {

  var hashList: Array[Array[Byte]] = null

  val hasher = new SingleThreadHasher()

  override def restore(): Future[Boolean] = {
    hasher.setup(universe)
    val os: OutputStream = null
    try {
      val hashList = if (fd.size > config.blockSize.bytes) {
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
        success = hash safeEquals fd.hash
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