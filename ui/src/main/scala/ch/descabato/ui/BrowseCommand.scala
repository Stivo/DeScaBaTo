package ch.descabato.ui

import ch.descabato.core.config.BackupFolderConfiguration
import ch.descabato.frontend.{BackupRelatedCommand, SimpleBackupFolderOption}

import scala.concurrent.Await
import scala.concurrent.duration._

/**
  * Created by Stivo on 26.12.2016.
  */
class BrowseCommand extends BackupRelatedCommand {
  type T = SimpleBackupFolderOption
  def newT(args: Seq[String]) = new SimpleBackupFolderOption(args)
  def start(t: T, conf: BackupFolderConfiguration) {
    printConfiguration(t)
    withUniverse(conf) { universe =>
      Await.result(universe.startup(), 1.minute)
      val bh = new Index(universe)
      BackupViewModel.index = bh

      ScalaFxGui.main(Array.empty)
    }
  }
}
