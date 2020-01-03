package ch.descabato.rocks

import ch.descabato.core.config.BackupFolderConfiguration
import ch.descabato.frontend.BackupRelatedCommand
import ch.descabato.frontend.SimpleBackupFolderOption

class UpgradeCommand extends BackupRelatedCommand {

  override val checkForUpgradeNeeded = false

  type T = SimpleBackupFolderOption

  def newT(args: Seq[String]) = new SimpleBackupFolderOption(args)

  def start(t: T, conf: BackupFolderConfiguration) {
    printConfiguration(t)
    logger.info("Upgrading from 0.6.0 to 0.8.0 backup format. This can not be reversed, and 0.6.0 will not be able to read the new format.")
    val bool = true || askUserYesNo("Are you sure?")
    if (bool) {
      val rocksEnv = RocksEnv(conf, false)
      new OldDataImporter(rocksEnv).importMetadata()
      val revisions = rocksEnv.rocks.getAllRevisions().size
      val allChunks = rocksEnv.rocks.getAllChunks()
      val totalSize = allChunks.map(_._2.lengthCompressed).sum
      val chunks = allChunks.size
      logger.info(s"Imported $revisions revisions and $chunks chunks with total compressed size of ${Utils.readableFileSize(totalSize)}. Should be about equal to the total size of all volumes.")
    } else {
      logger.info("Aborted.")
    }
  }
}