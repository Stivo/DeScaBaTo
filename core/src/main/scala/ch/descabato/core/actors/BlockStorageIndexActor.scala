package ch.descabato.core.actors

import java.io.File

import ch.descabato.core.model.{Block, StoredChunk}
import ch.descabato.core.{BlockStorage, JsonUser}
import ch.descabato.core_old.BackupFolderConfiguration
import ch.descabato.utils.Implicits._
import ch.descabato.utils.{BytesWrapper, FastHashMap, Hash}
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class BlockStorageIndexActor(val context: BackupContext, val chunkHandler: ChunkHandler) extends BlockStorage with JsonUser {

  val logger = LoggerFactory.getLogger(getClass)

  val config: BackupFolderConfiguration = context.config

  private var hasChanged = false

  private var previous: FastHashMap[StoredChunk] = new FastHashMap[StoredChunk]()
  private var current: FastHashMap[StoredChunk] = new FastHashMap[StoredChunk]()

  private var toBeStored: FastHashMap[Boolean] = new FastHashMap[Boolean]()

  def startup(): Future[Boolean] = {
    Future {
      val files = context.fileManager.volumeIndex.getFiles(config.folder)
      for (file <- files) {
        val seq = readJson[Seq[StoredChunk]](file)
        previous ++= seq.map(x => (x.hash, x))
      }
      true
    }
  }

  override def hasAlready(block: Block): Future[Boolean] = {
    val haveAlready = previous.safeContains(block.hash) || current.safeContains(block.hash) || toBeStored.safeContains(block.hash)
    if (!haveAlready) {
      toBeStored += block.hash -> true
      hasChanged = true
    }
    Future.successful(haveAlready)
  }

  override def save(storedChunk: StoredChunk): Future[Boolean] = {
    current += storedChunk.hash -> storedChunk
    toBeStored -= storedChunk.hash
    Future.successful(true)
  }

  def read(hash: Hash): Future[BytesWrapper] = {
    val chunk: StoredChunk = current.get(hash).orElse(previous.get(hash)).get
    chunkHandler.read(chunk)
  }

  override def finish(): Future[Boolean] = {
    if (hasChanged) {
      logger.info("Started writing blocks metadata")
      writeToJson(context.fileManager.volumeIndex.nextFile(), current.values.toSeq)
      logger.info("Done Writing blocks metadata")
    }
    Future.successful(true)
  }

}



