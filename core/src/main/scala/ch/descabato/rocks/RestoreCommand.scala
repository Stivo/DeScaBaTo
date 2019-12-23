package ch.descabato.rocks

import ch.descabato.core.config.BackupFolderConfiguration
import ch.descabato.core.util.FileManager
import ch.descabato.frontend.BackupRelatedCommand
import ch.descabato.frontend.RestoreConf


class RestoreCommand extends BackupRelatedCommand {

  type T = RestoreConf

  def newT(args: Seq[String]) = new RestoreConf(args)

  def start(t: T, conf: BackupFolderConfiguration) {
    printConfiguration(t)
    validateFilename(t.restoreToFolder)
    validateFilename(t.restoreInfo)
    val fm = new FileManager(conf)
    val restore = new DoRestore(conf)
    if (t.chooseDate()) {
      val options = fm.backup.getFiles().map(fm.backup.dateOfFile).zipWithIndex
      options.foreach {
        case (date, num) => println(s"[$num]: $date")
      }
      val option = askUser("Which backup would you like to restore from?").toInt
      restore.restoreFromDate(t, options.find(_._2 == option).get._1)
    } else if (t.restoreBackup.isSupplied) {
      val backupsFound = fm.backup.getFiles().filter(_.getName.equals(t.restoreBackup()))
      if (backupsFound.isEmpty) {
        println("Could not find described backup, these are your choices:")
        fm.backup.getFiles().foreach { f =>
          println(f)
        }
        throw new IllegalArgumentException("Backup not found")
      }
      restore.restoreFromDate(t, fm.backup.dateOfFile(backupsFound.head))

    } else {
      restore.restoreLatest(t)
    }
  }
}