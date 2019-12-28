package ch.descabato.it

//import java.io.File

import better.files._
import ch.descabato.core.config.BackupFolderConfiguration
import ch.descabato.rocks.Revision
import ch.descabato.rocks.RocksEnv
import ch.descabato.rocks.protobuf.keys.RevisionValue
import ch.descabato.utils.Utils

object DumpRocksdb extends Utils {

  def printRevisions(rocksEnv: RocksEnv): Unit = {
    val value: Seq[(Revision, RevisionValue)] = rocksEnv.rocks.getAllRevisions()
    value.map { case (revision, value) =>
      s"$revision:\n${value.configJson}\n" + value.files.map(_.toString).mkString("\n")
    }.foreach(logger.info(_))
  }

  def printFileStatuses(rocksEnv: RocksEnv): Unit = {
    val statuses = rocksEnv.rocks.getAllValueLogStatusKeys().map { case (key, status) =>
      s"$key: $status"
    }.mkString("\n")
    logger.info(s"Statuses:\n${statuses}")
  }

  def main(args: Array[String]): Unit = {
    val rocksEnv = RocksEnv(BackupFolderConfiguration(args(0).toFile.toJava), readOnly = true)
    try {
      printRevisions(rocksEnv)
      printFileStatuses(rocksEnv)
    } finally {
      rocksEnv.close()
    }
  }

}
