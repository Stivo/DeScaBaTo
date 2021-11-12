package ch.descabato.frontend.commands

import ch.descabato.core.actions.Backup
import ch.descabato.frontend.Command
import ch.descabato.frontend.CountConf
import ch.descabato.utils.Utils

class CountCommand extends Command with Utils {

  def start(t: CountConf): Unit = {
    logger.info(t.summary)
    val (folders, files) = Backup.listFiles(t.folderToCountIn(), t.ignoreFile.toOption)
    val count = files
      .foldLeft(Count(0, 0)) { (sum, file) =>
        sum.copy(files = sum.files + 1, size = sum.size + file.toFile.length())
      }
    logger.info(s"Would backup ${count.files} files and ${folders.length} folders, in total ${count.readableSize}")
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
