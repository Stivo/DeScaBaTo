package ch.descabato.it.tests

object DumpDbExport extends Utils {

  def printRevisions(rocksEnv: BackupEnv): Unit = {
    val value = rocksEnv.rocks.getAllRevisions()
    value.map { case (revision, value) =>
      s"$revision:\n${value.configJson}\n" + value.files.map(_.toString).mkString("\n")
    }.foreach(logger.info(_))
  }

  def printChunks(rocksEnv: BackupEnv): Unit = {
    val value = rocksEnv.rocks.getAllChunks()
    value.map { case (key, value) =>
      s"${key.hash.base64}: ${value}"
    }.foreach(logger.info(_))
  }

  def printFileStatuses(rocksEnv: BackupEnv): Unit = {
    val statuses = rocksEnv.rocks.getAllValueLogStatusKeys().map { case (key, status) =>
      s"$key: $status"
    }.mkString("\n")
    logger.info(s"Statuses:\n${statuses}")
  }

  def printDefaultKeys(rocksEnv: BackupEnv): Unit = {
    //    logger.info("Status: " + RepairLogic.readStatus(rocksEnv.rocks))
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
    for (rocksEnv <- BackupEnv(BackupFolderConfiguration(backupDestination, passphrase), readOnly = true, ignoreIssues = ignoreIssues).autoClosed) {
      printDefaultKeys(rocksEnv)
      printFileStatuses(rocksEnv)
      printRevisions(rocksEnv)
      printChunks(rocksEnv)
    }
    logger.info("End of dbexport content dump")
    logger.info("===========================================================")
  }
}
