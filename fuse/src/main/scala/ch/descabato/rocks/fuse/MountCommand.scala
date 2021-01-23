package ch.descabato.rocks.fuse

import java.nio.file.Paths
import ch.descabato.core.config.BackupFolderConfiguration
import ch.descabato.frontend.BackupFolderOption
import ch.descabato.frontend.BackupRelatedCommand
import ch.descabato.rocks.RocksEnv
import ch.descabato.utils.Utils
import org.rogach.scallop.ScallopConf
import org.rogach.scallop.ScallopOption

import java.awt.Desktop
import java.nio.file.Files
import java.nio.file.Path

class MountCommand extends BackupRelatedCommand {
  type T = FuseMountConf

  def newT(args: Seq[String]) = new FuseMountConf(args)

  def start(t: T, conf: BackupFolderConfiguration) {
    printConfiguration(t)

    val config = BackupFolderConfiguration(conf.folder, t.passphrase.toOption)
    val env = RocksEnv(config, readOnly = true)
    val reader = new BackupReader(env)
    val stub = new BackupFuseFS(reader)
    try {

      val path1: Path = parseAndCheckPath(t)
      logger.info(s"Mounting to $path1, this may take a while")
      new Thread() {
        override def run(): Unit = {
          while (!Files.exists(path1)) {
            Thread.sleep(2000)
          }
          if (Desktop.isDesktopSupported) {
            Desktop.getDesktop().open(path1.toFile())
          }
        }
      }.start()
      stub.mount(path1, true, false)

    } finally stub.umount()
  }

  def parseAndCheckPath(config: FuseMountConf): Path = {
    var path = config.mountFolder()
    if (Utils.isWindows) {
      if (path.length > 1) {
        path = path.substring(0, 1).toUpperCase() + ":\\"
        if (!path.matches("[A-Za-z]:\\\\")) {
          throw new IllegalArgumentException("Please specify a drive letter")
        }
        logger.info(s"""Corrected mount path for windows from "${config.mountFolder()}" to "$path". To avoid this message, just enter the drive letter by itself. """)
      } else {
        path = path.toUpperCase() + ":\\"
        if (!path.matches("[A-Za-z]:\\\\")) {
          throw new IllegalArgumentException("Please specify a drive letter")
        }
      }
    }
    val path1 = Paths.get(path)
    if (Files.exists(path1)) {
      throw new IllegalArgumentException(s"Folder or drive may not exist, $path1 exists already")
    }
    path1
  }

}

class FuseMountConf(args: Seq[String]) extends ScallopConf(args) with BackupFolderOption {
  val mountFolder: ScallopOption[String] = opt[String](descr = "The folder to mount the contents on (on windows: drive letter)", required = true)
}
