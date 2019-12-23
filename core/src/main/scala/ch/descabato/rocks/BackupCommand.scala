package ch.descabato.rocks

import java.io.File
import java.io.FileOutputStream
import java.io.PrintStream

import ch.descabato.core.config.BackupFolderConfiguration
import ch.descabato.frontend.BackupConf
import ch.descabato.frontend.BackupRelatedCommand
import ch.descabato.utils.Utils


class BackupCommand extends BackupRelatedCommand with Utils {

  val suffix: String = if (Utils.isWindows) ".bat" else ""

  def writeBat(t: T, conf: BackupFolderConfiguration, args: Seq[String]): Unit = {
    var path = new File(s"descabato$suffix").getCanonicalFile
    // TODO the directory should be determined by looking at the classpath
    if (!path.exists) {
      path = new File(path.getParent() + "/bin", path.getName)
    }
    val line = s"$path backup " + args.map {
      case x if x.contains(" ") => s""""$x""""
      case x => x
    }.mkString(" ")

    def writeTo(bat: File) {
      if (!bat.exists) {
        val ps = new PrintStream(new FileOutputStream(bat))
        ps.print(line)
        ps.close()
        l.info("A file " + bat + " has been written to execute this backup again")
      }
    }

    writeTo(new File(".", conf.folder.getName() + suffix))
    writeTo(new File(conf.folder, "_" + conf.folder.getName() + suffix))
  }

  type T = BackupConf

  def newT(args: Seq[String]) = new BackupConf(args)

  def start(t: T, conf: BackupFolderConfiguration) {
    printConfiguration(t)

    if (!t.noScriptCreation()) {
      writeBat(t, conf, lastArgs)
    }
    new RunBackup(conf).run(t.folderToBackup())
  }

  override def needsExistingBackup = false
}
