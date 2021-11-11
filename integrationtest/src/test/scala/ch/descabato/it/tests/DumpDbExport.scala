package ch.descabato.it.tests

import ch.descabato.core.config.BackupFolderConfiguration
import ch.descabato.core.model.BackupEnv
import ch.descabato.utils.Utils
import better.files._
import java.io

object DumpDbExport extends Utils {

  def printRevisions(backupEnv: BackupEnv): Unit = {
    val value = backupEnv.rocks.getAllRevisions()
    value.map { case (revision, value) =>
      s"$revision:\n${value.configJson}\n" + value.files.map(_.toString).mkString("\n")
    }.foreach(logger.info(_))
  }

  def printChunks(backupEnv: BackupEnv): Unit = {
    val value = backupEnv.rocks.getAllChunks()
    value.map { case (key, value) =>
      s"${key.hash.base64}: ${value}"
    }.foreach(logger.info(_))
  }

  def printFileStatuses(backupEnv: BackupEnv): Unit = {
    val statuses = backupEnv.rocks.getAllValueLogStatusKeys().map { case (key, status) =>
      s"$key: $status"
    }.mkString("\n")
    logger.info(s"Statuses:\n${statuses}")
  }

  def printDefaultKeys(backupEnv: BackupEnv): Unit = {
    //    logger.info("Status: " + RepairLogic.readStatus(backupEnv.rocks))
  }

  def main(args: Array[String]): Unit = {
    val passphrase = if (args.size > 1) {
      Some(args(1))
    } else {
      None
    }
    val backupDestination = args(0).toFile.toJava
    dumpDbExport(backupDestination, passphrase)
  }

  def dumpDbExport(backupDestination: io.File, passphrase: Option[String] = None, ignoreIssues: Boolean = false) = {
    logger.info("===========================================================")
    logger.info("Start of dbexport content dump")
    for (backupEnv <- BackupEnv(BackupFolderConfiguration(backupDestination, passphrase), readOnly = true, ignoreIssues = ignoreIssues).autoClosed) {
      printDefaultKeys(backupEnv)
      printFileStatuses(backupEnv)
      printRevisions(backupEnv)
      printChunks(backupEnv)
    }
    logger.info("End of dbexport content dump")
    logger.info("===========================================================")
  }
}
