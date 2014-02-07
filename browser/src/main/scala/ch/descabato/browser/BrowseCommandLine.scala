package ch.descabato.browser

import ch.descabato.SimpleBackupFolderOption
import ch.descabato.BackupFolderConfiguration
import ch.descabato.BackupRelatedCommand
import ch.descabato.ZipBlockStrategy
import pl.otros.vfs.browser.demo.TestBrowser
import ch.descabato.CLI



class BrowseCommand extends BackupRelatedCommand {
  type T = SimpleBackupFolderOption
  def newT(args: Seq[String]) = new SimpleBackupFolderOption(args)
  def start(t: T, conf: BackupFolderConfiguration) {
    println(t.summary)
    val bh = new VfsIndex(conf) with ZipBlockStrategy
    bh.registerIndex()
    val path = conf.folder.getCanonicalPath()
    val url = s"backup:file://$path!"
    BackupBrowser.main2(Array(url))
  }
}

object Main {
  def main(args: Array[String]) {
    CLI.main(args)
  }
}