package ch.descabato.it.rocks


import java.io

import better.files._
import ch.descabato.core.config.BackupFolderConfiguration
import ch.descabato.rocks.ChunkKey
import ch.descabato.rocks.RepairLogic
import ch.descabato.rocks.Revision
import ch.descabato.rocks.RocksEnv
import ch.descabato.rocks.protobuf.keys.RevisionValue
import ch.descabato.rocks.protobuf.keys.ValueLogIndex
import ch.descabato.utils.Utils

object DumpRocksdb extends Utils {

  def printRevisions(rocksEnv: RocksEnv): Unit = {
    val value = rocksEnv.rocks.getAllRevisions()
    value.map { case (revision, value) =>
      s"$revision:\n${value.configJson}\n" + value.files.map(_.toString).mkString("\n")
    }.foreach(logger.info(_))
  }

  def printChunks(rocksEnv: RocksEnv): Unit = {
    val value = rocksEnv.rocks.getAllChunks()
    value.map { case (key, value) =>
      s"${key.hash.base64}: ${value}"
    }.foreach(logger.info(_))
  }

  def printFileStatuses(rocksEnv: RocksEnv): Unit = {
    val statuses = rocksEnv.rocks.getAllValueLogStatusKeys().map { case (key, status) =>
      s"$key: $status"
    }.mkString("\n")
    logger.info(s"Statuses:\n${statuses}")
  }

  def printDefaultKeys(rocksEnv: RocksEnv): Unit = {
//    logger.info("Status: " + RepairLogic.readStatus(rocksEnv.rocks))
  }

  def main(args: Array[String]): Unit = {
    val passphrase = if (args.size > 1) {
      Some(args(1))
    } else {
      None
    }
    val backupDestination = args(0).toFile.toJava
    dumpRocksDb(backupDestination, passphrase)
  }

  def dumpRocksDb(backupDestination: io.File, passphrase: Option[String] = None, ignoreIssues: Boolean = false) = {
    logger.info("===========================================================")
    logger.info("Start of rocksdb content dump")
    for (rocksEnv <- RocksEnv(BackupFolderConfiguration(backupDestination, passphrase), readOnly = true, ignoreIssues = ignoreIssues).autoClosed) {
      printDefaultKeys(rocksEnv)
      printFileStatuses(rocksEnv)
      printRevisions(rocksEnv)
      printChunks(rocksEnv)
    }
    logger.info("End of rocksdb content dump")
    logger.info("===========================================================")
  }
}
