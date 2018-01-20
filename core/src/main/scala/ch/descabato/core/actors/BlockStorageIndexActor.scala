package ch.descabato.core.actors

import java.io.File

import akka.util.ByteString
import ch.descabato.core.{BlockStorage, JsonUser}
import ch.descabato.core.model.{Block, StoredChunk}
import ch.descabato.core_old.BackupFolderConfiguration
import ch.descabato.utils.{BytesWrapper, Hash}
import org.slf4j.LoggerFactory
import ch.descabato.utils.Implicits._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class BlockStorageIndexActor(val config: BackupFolderConfiguration, val chunkHandler: ChunkHandler) extends BlockStorage with JsonUser {

  val logger = LoggerFactory.getLogger(getClass)

  private var hasChanged = false

  private var map: Map[Hash, StoredChunk] = Map.empty

  private var toBeStored: Set[Hash] = Set.empty

  private val value = "blockIndex"
  val file = new File(config.folder, value + ".json")

  def startup(): Future[Boolean] = {
    Future {
      if (file.exists()) {
        val seq = readJson[Seq[StoredChunk]](file)
        map = seq.map(x => (x.hash, x)).toMap
      }
      true
    }
  }

  override def hasAlready(block: Block): Future[Boolean] = {
    val haveAlready = map.safeContains(block.hash) || toBeStored.safeContains(block.hash)
    if (!haveAlready) {
      toBeStored += block.hash
      hasChanged = true
    }
    Future.successful(haveAlready)
  }

  override def save(storedChunk: StoredChunk): Future[Boolean] = {
    map += storedChunk.hash -> storedChunk
    toBeStored -= storedChunk.hash
    Future.successful(true)
  }

  def read(hash: Hash): Future[BytesWrapper] = {
    val chunk: StoredChunk = map(hash)
    chunkHandler.read(chunk)
  }

  override def finish(): Future[Boolean] = {
    if (hasChanged) {
      logger.info("Started writing blocks metadata")
      writeToJson(file, map.values)
      logger.info("Done Writing blocks metadata")
    }
    Future.successful(true)
  }

}



