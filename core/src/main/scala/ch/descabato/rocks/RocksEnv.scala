package ch.descabato.rocks

import ch.descabato.core.config.BackupFolderConfiguration
import ch.descabato.core.util.FileManager
import ch.descabato.utils.Utils
import com.typesafe.scalalogging.LazyLogging

import java.io.File

/**
 * This is the rocks env before rocks is created. It is necessary as an intermediate step
 * to allow the repair logic to act before rocks is created.
 *
 * @param config   The configuration
 * @param readOnly Whether this should be read only
 */
class RocksEnvInit(val config: BackupFolderConfiguration,
                   val readOnly: Boolean) extends Utils {
  val fileManager: FileManager = new FileManager(config)
  val backupFolder: File = config.folder
  val rocksFolder = new File(backupFolder, "rocks")

  if (!backupFolder.exists()) {
    logger.info(s"Creating folders for backup at ${backupFolder}")
    backupFolder.mkdir()
  }
  val startedWithoutRocksdb: Boolean = !rocksFolder.exists()

}

class RocksEnv(val rocksEnvInit: RocksEnvInit,
               val rocks: KeyValueStore) extends LazyLogging with AutoCloseable {

  def config: BackupFolderConfiguration = rocksEnvInit.config

  def fileManager: FileManager = rocksEnvInit.fileManager

  private var readerInitialized = false
  lazy val reader: ValueLogReader = {
    readerInitialized = true
    new ValueLogReader(this)
  }

  override def close(): Unit = {
    if (readerInitialized) {
      logger.info("Closing reader now")
      reader.close()
    }
  }
}

object RocksEnv extends LazyLogging {
  def apply(config: BackupFolderConfiguration, readOnly: Boolean, ignoreIssues: Boolean = false): RocksEnv = {
    val rocksEnvInit = new RocksEnvInit(config, readOnly)
    val kvs = new RepairLogic(rocksEnvInit).initialize()
    new RocksEnv(rocksEnvInit, kvs)
  }
}