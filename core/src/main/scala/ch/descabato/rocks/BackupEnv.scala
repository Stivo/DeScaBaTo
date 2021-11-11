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
class BackupEnvInit(val config: BackupFolderConfiguration,
                    val readOnly: Boolean) extends Utils {
  val fileManager: FileManager = new FileManager(config)
  val backupFolder: File = config.folder

  if (!backupFolder.exists()) {
    logger.info(s"Creating folders for backup at ${backupFolder}")
    backupFolder.mkdir()
  }

}

class BackupEnv(val backupEnvInit: BackupEnvInit,
                val rocks: KeyValueStore) extends LazyLogging with AutoCloseable {

  def config: BackupFolderConfiguration = backupEnvInit.config

  def fileManager: FileManager = backupEnvInit.fileManager

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

object BackupEnv extends LazyLogging {
  def apply(config: BackupFolderConfiguration, readOnly: Boolean, ignoreIssues: Boolean = false): BackupEnv = {
    val backupEnvInit = new BackupEnvInit(config, readOnly)
    val kvs = new RepairLogic(backupEnvInit).initialize()
    new BackupEnv(backupEnvInit, kvs)
  }
}