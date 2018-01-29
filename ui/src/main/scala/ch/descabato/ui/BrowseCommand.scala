package ch.descabato.ui

import ch.descabato.core_old.BackupFolderConfiguration
import ch.descabato.frontend.{BackupRelatedCommand, SimpleBackupFolderOption}

/**
  * Created by Stivo on 26.12.2016.
  */
class BrowseCommand extends BackupRelatedCommand {
  type T = SimpleBackupFolderOption
  def newT(args: Seq[String]) = new SimpleBackupFolderOption(args)
  def start(t: T, conf: BackupFolderConfiguration) {
    println(t.summary)
    withUniverse(conf) { universe =>
      val bh = new Index(universe)
      BackupViewModel.index = bh

      ScalaFxGui.main(Array.empty)
    }
  }
}
