package ch.descabato.frontend.commands

import better.files._
import ch.descabato.core.config.BackupFolderConfiguration
import ch.descabato.core.model.BackupEnv
import ch.descabato.frontend.BackupRelatedCommand
import ch.descabato.frontend.SimpleBackupFolderOption
import ch.descabato.remote.RemoteUploader
import ch.descabato.utils.Utils

class UploadCommand extends BackupRelatedCommand with Utils {
  override type T = SimpleBackupFolderOption

  override def newT(args: Seq[String]): T = new SimpleBackupFolderOption(args)

  override def start(t: T, conf: BackupFolderConfiguration): Unit = {
    var finished, failed = 0
    for {
      backupEnv <- BackupEnv(conf, true).autoClosed
      uploader <- new RemoteUploader(backupEnv).autoClosed
    } {
      var continue = true
      while (continue) {
        val (finishedNow, failedNow) = uploader.uploadAllRemaining(10)
        finished += finishedNow
        failed += failedNow
        continue = finishedNow + failedNow > 0
      }
    }
    logger.info(s"All done, had $failed failures for $finished files")
  }
}
