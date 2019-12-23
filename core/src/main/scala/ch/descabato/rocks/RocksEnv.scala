package ch.descabato.rocks

import java.io.File

import ch.descabato.core.config.BackupFolderConfiguration
import com.typesafe.scalalogging.LazyLogging


class RocksEnv(private val config: BackupFolderConfiguration) extends LazyLogging {
  val backupFolder: File = config.folder
  lazy val reader: ValueLogReader = new ValueLogReader(this)
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
  val rocks: RocksDbKeyValueStore = RocksDbKeyValueStore(rocksFolder)
}

object RocksEnv extends LazyLogging {
  def apply(c: BackupFolderConfiguration): RocksEnv = {
    new RocksEnv(c)
  }
}