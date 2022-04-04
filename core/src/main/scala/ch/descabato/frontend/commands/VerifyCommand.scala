package ch.descabato.frontend.commands

import ch.descabato.Main
import ch.descabato.core.config.BackupFolderConfiguration
import ch.descabato.frontend.BackupRelatedCommand
import ch.descabato.frontend.VerifyConf


class VerifyCommand extends BackupRelatedCommand {
  type T = VerifyConf

  def newT(args: Seq[String]) = new VerifyConf(args)

  def start(t: T, conf: BackupFolderConfiguration): Unit = {
    printConfiguration(t)
    // TODO reimplement
    ???
    //    val counter = new DoVerify(conf).verifyAll(t)
    //    Main.lastErrors = counter.count
    //    if (counter.count != 0)
    //      Main.exit(counter.count.toInt)
  }
}
