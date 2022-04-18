package ch.descabato.frontend.commands

import ch.descabato.core.actions.Backup
import ch.descabato.core.model.Size
import ch.descabato.frontend.Command
import ch.descabato.frontend.CountConf
import ch.descabato.utils.Utils

class CountCommand extends Command with Utils {

  def start(t: CountConf): Unit = {
    logger.info(t.summary)
    var totalFiles = 0L
    var totalSize = 0L
    for (folder <- t.foldersToCountIn()) {
      val visitor = Backup.listFiles(folder, t.ignoreFile.toOption)
      totalSize += visitor.bytesCounter.maxValue
      totalFiles += visitor.fileCounter.maxValue
      logger.info(s"$folder: Would backup ${visitor.fileCounter.maxValue} files and ${visitor.dirs.size} folders, in total ${Size(visitor.bytesCounter.maxValue)}.")
    }
    logger.info(s"Would backup ${totalFiles} files and ${Size(totalSize)} bytes in total")
  }

  override def execute(args: Seq[String]): Unit = {
    val conf = new CountConf(args)
    conf.verify()
    start(conf)
  }
}
