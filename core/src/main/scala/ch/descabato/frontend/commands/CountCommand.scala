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
      val (folders, files) = Backup.listFiles(folder, t.ignoreFile.toOption)
      val count = files
        .foldLeft(Count(0, 0)) { (sum, file) =>
          sum.copy(files = sum.files + 1, size = sum.size + file.toFile.length())
        }
      totalFiles += count.files
      totalSize += count.size
      logger.info(s"Would backup ${count.files} files and ${folders.length} folders, in total ${count.readableSize} for folder $folder")
    }
    logger.info(s"Would backup ${totalFiles} files and ${Size(totalSize)} bytes in total")
  }

  override def execute(args: Seq[String]): Unit = {
    val conf = new CountConf(args)
    conf.verify()
    start(conf)
  }
}

case class Count(files: Long, size: Long) {
  def readableSize: String = Utils.readableFileSize(size)
}
