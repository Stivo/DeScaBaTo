package ch.descabato.frontend.commands

import better.files._
import ch.descabato.core.actions.DoRestore
import ch.descabato.core.config.BackupFolderConfiguration
import ch.descabato.frontend.BackupRelatedCommand
import ch.descabato.frontend.RestoreConf

class RestoreCommand extends BackupRelatedCommand {

  type T = RestoreConf

  def newT(args: Seq[String]) = new RestoreConf(args)

  def start(t: T, conf: BackupFolderConfiguration): Unit = {
    printConfiguration(t)
    validateFilename(t.restoreToFolder)
    validateFilename(t.restoreInfo)
    for (restore <- new DoRestore(conf).autoClosed) {
      if (t.chooseDate()) {
        //        val options = fm.backup.getFiles().map(fm.backup.dateOfFile).zipWithIndex
        //        options.foreach {
        //          case (date, num) => println(s"[$num]: $date")
        //        }
        //        val option = askUser("Which backup would you like to restore from?").toInt
        //        restore.restoreRevision(t, options.find(_._2 == option).get._1)
        ???
      } else if (t.restoreBackup.isSupplied) {
        restore.restoreByRevision(t, t.restoreBackup().toInt)
      } else {
        restore.restoreLatest(t)
      }
    }
  }
}