package ch.descabato.browser

import ch.descabato.SimpleBackupFolderOption
import ch.descabato.BackupFolderConfiguration
import ch.descabato.BackupRelatedCommand
import ch.descabato.ZipBlockStrategy
import pl.otros.vfs.browser.demo.TestBrowser



class BrowseCommand(val args: Seq[String]) extends BackupRelatedCommand {
  type T = SimpleBackupFolderOption
  val newT = new SimpleBackupFolderOption(args)
  def start(t: T, conf: BackupFolderConfiguration) {
    t.afterInit
    println(t.summary)
    val bh = new VfsIndex(conf) with ZipBlockStrategy
    bh.registerIndex()
    val path = conf.folder.getAbsolutePath()
    val url = s"backup:file://$path!"
    
    TestBrowser.main(Array(url))

  }
}
