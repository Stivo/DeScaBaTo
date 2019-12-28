package ch.descabato.rocks

import java.io.File

import ch.descabato.core.config.BackupFolderConfiguration
import ch.descabato.core.util.FileManager
import com.typesafe.scalalogging.LazyLogging


class RocksEnv(val config: BackupFolderConfiguration, private val readOnly: Boolean) extends LazyLogging with AutoCloseable {
  val backupFolder: File = config.folder

  def relativize(file: File): String = {
    backupFolder.toPath.relativize(file.toPath).toString
  }

  private var readerInitialized = false
  lazy val reader: ValueLogReader = {
    readerInitialized = true
    new ValueLogReader(this)
  }
  val valuelogsFolder = new File(backupFolder, "valuelogs")
  val rocksFolder = new File(backupFolder, "rocks")
  if (!backupFolder.exists()) {
    logger.info(s"Creating folders for backup at ${backupFolder}")
    backupFolder.mkdir()
  }
  if (!valuelogsFolder.exists()) {
    logger.info(s"Creating folders for valuelogs at ${valuelogsFolder}")
    backupFolder.mkdir()
  }
  val rocks: RocksDbKeyValueStore = RocksDbKeyValueStore(rocksFolder, readOnly)

  val fileManager = new FileManager(config)

  override def close(): Unit = {
    if (readerInitialized) {
      logger.info("Closing reader now")
      reader.close()
    }
    logger.info("Closing rocksdb now")
    rocks.close()
  }
}

object RocksEnv extends LazyLogging {
  def apply(c: BackupFolderConfiguration, readOnly: Boolean): RocksEnv = {
    new RocksEnv(c, readOnly)
  }
}