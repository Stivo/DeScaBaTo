package ch.descabato.rocks.fuse

import java.nio.file.Paths
import ch.descabato.core.config.BackupFolderConfiguration
import ch.descabato.core.model.BackupEnv
import ch.descabato.core.model.Size
import ch.descabato.frontend.BackupFolderOption
import ch.descabato.frontend.BackupRelatedCommand
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
    val env = BackupEnv(config, readOnly = true)
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

  def printFilesPerVolume(reader: BackupReader): Unit = {
    val volumes = reader.chunksByVolume
    //    volumes.view.mapValues { m =>
    //      (m.values.map(_.lengthCompressed).sum,m.values.map(_.lengthUncompressed).sum)
    //    }.foreach { case (num, total) =>
    //      println(num + ": " + Utils.readableFileSize(total._1) +" "+Utils.readableFileSize(total._2))
    //    }

    val rev = reader.revisions.maxBy(_.revision.number)
    println(rev.revision)
    for (x <- volumes.keys if x >= 584) {
      println(s"volume ${x}")
      val map = reader.fileMetadataValuesByVolume(rev.revision, x)
      val sortedBySize = map.toSeq.sortBy(-_._2._1)
      sortedBySize.takeWhile(_._2._1 > 100 * 1024).foreach { case (path, length) =>
        println(path + " " + Size(length._1) + " " + length._2.mkString(" "))
      }
      println()
    }

  }

}

class FuseMountConf(args: Seq[String]) extends ScallopConf(args) with BackupFolderOption {
  val mountFolder: ScallopOption[String] = opt[String](descr = "The folder to mount the contents on (on windows: drive letter)", required = true)
}
