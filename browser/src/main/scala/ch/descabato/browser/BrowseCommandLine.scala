package ch.descabato.browser

import ch.descabato.frontend.{CLI, SimpleBackupFolderOption, BackupRelatedCommand}
import ch.descabato.core.BackupFolderConfiguration


class BrowseCommand extends BackupRelatedCommand {
  type T = SimpleBackupFolderOption
  def newT(args: Seq[String]) = new SimpleBackupFolderOption(args)
  def start(t: T, conf: BackupFolderConfiguration) {
    println(t.summary)
    withUniverse(conf, false) { universe =>kh
      val bh = new VfsIndex(universe)
      bh.registerIndex()
      val path = conf.folder.getCanonicalPath()
      val url = s"backup:file://$path!"
      BackupBrowser.main2(Array(url))
    }
  }
}

object Main {
  def main(args: Array[String]) {
    CLI.main(args)
  }
}