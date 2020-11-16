package ch.descabato.rocks.fuse

import java.nio.file.Paths

import ch.descabato.core.config.BackupFolderConfiguration
import ch.descabato.frontend.BackupFolderOption
import ch.descabato.frontend.BackupRelatedCommand
import ch.descabato.rocks.RocksEnv
import org.rogach.scallop.ScallopConf
import org.rogach.scallop.ScallopOption

/**
 * Created by Stivo on 26.12.2016.
 */
class ServeCommand extends BackupRelatedCommand {
  type T = FuseMountConf

  def newT(args: Seq[String]) = new FuseMountConf(args)

  def start(t: T, conf: BackupFolderConfiguration) {
    printConfiguration(t)

    val config = BackupFolderConfiguration(conf.folder)
    val env = RocksEnv(config, readOnly = true)
    val reader = new BackupReader(env)
    val stub = new TestFuse(reader)
    try {
      val path = t.mountFolder()
      stub.mount(Paths.get(path), true, false)
    } finally stub.umount()
  }
}

class FuseMountConf(args: Seq[String]) extends ScallopConf(args) with BackupFolderOption {
  val mountFolder: ScallopOption[String] = opt[String](descr = "The folder to mount the contents on")
}
