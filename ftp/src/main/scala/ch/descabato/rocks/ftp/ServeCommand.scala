package ch.descabato.rocks.ftp

import ch.descabato.core.config.BackupFolderConfiguration
import ch.descabato.frontend.BackupRelatedCommand
import ch.descabato.frontend.CreateBackupOptions
import org.rogach.scallop.ScallopConf
import org.rogach.scallop.ScallopOption

/**
 * Created by Stivo on 26.12.2016.
 */
class ServeCommand extends BackupRelatedCommand {
  type T = FtpServeConf

  def newT(args: Seq[String]) = new FtpServeConf(args)

  def start(t: T, conf: BackupFolderConfiguration) {
    printConfiguration(t)
    // TODO make a new command
  }
}

class FtpServeConf(args: Seq[String]) extends ScallopConf(args) with CreateBackupOptions {
  val port: ScallopOption[Int] = opt[Int](descr = "The port to serve the ftp files on", default = Some(8021))
}
