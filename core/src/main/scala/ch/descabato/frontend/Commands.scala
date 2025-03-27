package ch.descabato.frontend

import ch.descabato.CompressionMode
import ch.descabato.HashAlgorithm
import ch.descabato.Main
import ch.descabato.RemoteMode
import ch.descabato.RestoreCommand
import ch.descabato.core.BackupException
import ch.descabato.core.ExceptionFactory
import ch.descabato.core.MisconfigurationException
import ch.descabato.core.actions.BackupCommand
import ch.descabato.core.actions.CheckFilesCommand
import ch.descabato.core.actions.VerifyCommand
import ch.descabato.core.config.BackupConfigurationHandler
import ch.descabato.core.config.BackupFolderConfiguration
import ch.descabato.core.model.Size
import ch.descabato.core.util.FileManager
import ch.descabato.frontend.ScallopConverters.*
import ch.descabato.frontend.commands.CountCommand
import ch.descabato.frontend.commands.UploadCommand
import ch.descabato.utils.BuildInfo
import ch.descabato.utils.Implicits.*
import ch.descabato.utils.Utils
import org.rogach.scallop.*
import org.rogach.scallop.exceptions.ScallopException

import java.io.File
import java.lang.reflect.InvocationTargetException
import java.nio.file.FileSystems

object ScallopConverters {
  def singleArgConverter[A](conv: String => A, msg: String = "wrong arguments format"): ValueConverter[A] = new ValueConverter[A] {
    def parse(s: List[(String, List[String])]): Either[String, Option[A]] = {
      s match {
        case (_, i :: Nil) :: Nil =>
          try {
            Right(Some(conv(i)))
          } catch {
            case _: Throwable => Left(msg)
          }
        case Nil => Right(None)
        case _ => Left("you should provide exactly one argument for this option")
      }
    }

    val argType: ArgType.V = ArgType.SINGLE
  }

  implicit val modeConverter: ValueConverter[CompressionMode] = singleArgConverter[CompressionMode](CompressionMode.valueOf, "Should be one of " + CompressionMode.values.mkString(", "))
  implicit val hashAlgorithmConverter: ValueConverter[HashAlgorithm] = singleArgConverter[HashAlgorithm](HashAlgorithm.valueOf, "Should be one of " + HashAlgorithm.values.mkString(", "))
  implicit val remoteModeConverter: ValueConverter[RemoteMode] = singleArgConverter[RemoteMode](RemoteMode.fromCli, RemoteMode.message)
  implicit val sizeConverter: ValueConverter[Size] = singleArgConverter[Size](x => Size(x))

}

// Parsing classes

trait RedundancyOptions extends BackupFolderOption {
  // these have been disabled for a long time
  //  val metadataRedundancy = opt[Int](default = Some(20))
  //  val volumeRedundancy = opt[Int](default = Some(5))
  //  val noRedundancy = opt[Boolean](default = Some(false))
}

trait ChangeableBackupOptions extends BackupFolderOption with RedundancyOptions with NoGuiOption {
  val keylength: ScallopOption[Int] = opt[Int](descr = "Length of the AES encryption key", default = Some(128))
  val volumeSize: ScallopOption[Size] = opt[Size](descr = "Maximum size of the main data files", default = Some(Size("500MiB")))
  val noScriptCreation: ScallopOption[Boolean] = opt[Boolean](default = Some(false), descr = "Disables creating a script to repeat the backup.")
  //  val renameDetection = opt[Boolean](hidden = true, default = Some(false))
  val dontSaveSymlinks: ScallopOption[Boolean] = opt[Boolean](default = Some(false), descr = "Disable backing up symlinks")
  val compression: ScallopOption[CompressionMode] = opt[CompressionMode](descr = "The compressor to use. Smart chooses best compressor by file extension", default = Some(CompressionMode.smart))
  val ignoreFile: ScallopOption[File] = opt[File](descr = "File with ignore patterns", default = None)
}

trait CreateBackupOptions extends ChangeableBackupOptions {
  val hashAlgorithm: ScallopOption[HashAlgorithm] = opt[HashAlgorithm](descr = "The hash algorithm to use for deduplication.", default = Some(HashAlgorithm.sha3_256))
  val remoteUri: ScallopOption[String] = opt[String](hidden = true)
  val remoteMode: ScallopOption[RemoteMode] = opt[RemoteMode](hidden = true)
  codependent(remoteUri, remoteMode)
}

trait ProgramOption extends ScallopConf {
  val logfile: ScallopOption[String] = opt[String](descr = "Destination of the logfile of this backup, as an absolute or relative path")

  def printConfiguration(): Unit = {
    println(this.summary)
  }

  override def verify(): Unit = {
    super.verify()
    logfile.foreach { x =>
      if (x.contains("/") || x.contains("\\")) {
        throw new IllegalArgumentException("Can only specify a filename, not an absolute path")
      }
    }
  }

}

trait NoGuiOption extends ScallopConf {
  val noGui: ScallopOption[Boolean] = opt[Boolean](noshort = true, descr = "Disables the progress report window")
}

trait BackupFolderOption extends ProgramOption {
  val passphrase: ScallopOption[String] = opt[String](descr = "The password to use for the backup. If none is supplied, encryption is turned off", default = None)
  val backupDestination: ScallopOption[File] = trailArg[String](descr = "Root folder of the backup", required = true).map(new File(_).getCanonicalFile())

  override def printConfiguration(): Unit = {
    if (this.passphrase.isDefined) {
      println(this.filteredSummary(Set(this.passphrase.name)))
    } else {
      println(this.summary)
    }
  }

}

class UploadConf(args: Seq[String]) extends ScallopConf(args) with BackupFolderOption
  with BackupConfCommandCreator {
  override def runCommand(backupFolderConf: BackupFolderConfiguration): Unit = {
    new UploadCommand(this, backupFolderConf).run()
  }

  override def needsExistingBackup: Boolean = true
}

class BackupConf(args: Seq[String]) extends ScallopConf(args) with CreateBackupOptions {
  val folderToBackup: ScallopOption[File] = trailArg[File](descr = "Folder to be backed up").map(_.getCanonicalFile())
}

class CountConf(args: Seq[String]) extends ScallopConf(args) with ProgramOption with SimpleCommandCreator {
  val dontSaveSymlinks: ScallopOption[Boolean] = opt[Boolean](default = Some(false), descr = "Disable backing up symlinks")
  val ignoreFile: ScallopOption[File] = opt[File](descr = "File with ignore patterns", default = None)
  val foldersToCountIn: ScallopOption[List[File]] = trailArg[List[File]](descr = "Folder to count files in").map(_.map(_.getCanonicalFile()))

  def runCommand(): Unit = new CountCommand().start(this)
}

class GenericConf(args: Seq[String], command: GenericCommand) extends ScallopConf(args) with SimpleCommandCreator {

  override def verify(): Unit = {}

  def runCommand(): Unit = command.execute(args)
}

class MultipleBackupConf(args: Seq[String]) extends ScallopConf(args) with CreateBackupOptions with BackupConfCommandCreator {
  val foldersToBackup: ScallopOption[List[File]] = trailArg[List[File]](descr = "Folders to be backed up").map(_.map(_.getCanonicalFile()))

  override def runCommand(backupFolderConf: BackupFolderConfiguration): Unit =
    new BackupCommand(this, backupFolderConf).run()

  def needsExistingBackup: Boolean = false
}

class RestoreConf(args: Seq[String]) extends ScallopConf(args) with BackupFolderOption with NoGuiOption
  with BackupConfCommandCreator {
  val restoreToOriginalPath: ScallopOption[Boolean] = opt[Boolean](descr = "Restore files to original path.")
  val restoreToFolder: ScallopOption[String] = opt[String](descr = "Restore to a given folder")
  val restoreBackup: ScallopOption[String] = opt[String](descr = "Filename of the backup to restore.")
  val restoreInfo: ScallopOption[String] = opt[String](descr = "Destination of a short summary file of the restore process.")
  //  val overwriteExisting = toggle(default = Some(false))
  //  val pattern = opt[String]()
  requireOne(restoreToOriginalPath, restoreToFolder)

  override def runCommand(backupFolderConf: BackupFolderConfiguration): Unit =
    new RestoreCommand(this, backupFolderConf).run()

  def needsExistingBackup: Boolean = true

}

class VerifyConf(args: Seq[String]) extends ScallopConf(args) with BackupFolderOption with NoGuiOption
  with BackupConfCommandCreator {
  val percentOfFilesToCheck: ScallopOption[Int] = opt[Int](default = Some(5), descr = "How many percent of files to check")
  val checkFirstOfEachVolume: ScallopOption[Boolean] = opt[Boolean](default = Some(false), descr = "Check whether the first entry of each volume is correct or not")
  validate(percentOfFilesToCheck) {
    case x if x >= 0 && x <= 100 => Right(())
    case _ => Left("Needs to be percent")
  }

  override def runCommand(backupFolderConf: BackupFolderConfiguration): Unit =
    new VerifyCommand(this, backupFolderConf).run()

  def needsExistingBackup: Boolean = true

}

class CheckFilesConf(args: Seq[String]) extends ScallopConf(args) with BackupFolderOption with NoGuiOption
  with BackupConfCommandCreator {

  override def runCommand(backupFolderConf: BackupFolderConfiguration): Unit =
    new CheckFilesCommand(this, backupFolderConf).run()

  def needsExistingBackup: Boolean = true

}

trait GenericCommand {
  def execute(args: Seq[String]): Unit
}

class HelpCommand(commandRunner: CommandRunner) extends GenericCommand {
  private val allCommands = commandRunner.allCommands()

  override def execute(args: Seq[String]): Unit = {
    args.toList match {
      // TODO this won't work
      case command :: _ if allCommands.safeContains(command) =>
        new CommandRunner(command :: "--help" :: Nil).runCommand()
      case _ =>
        val commands = allCommands.mkString(", ")
        println(
          s"""|Welcome to DeScaBaTo ${BuildInfo.version}.
              |The available commands are: $commands
              |For further help about a specific command type 'help backup' or 'backup --help'.
              |For general usage guide go to https://github.com/Stivo/DeScaBaTo""".stripMargin
        )
    }
  }
}

class VersionCommand extends GenericCommand {

  override def execute(args: Seq[String]): Unit = {
    println(
      s"""|DeScaBaTo version ${BuildInfo.version}
          |Scala ${BuildInfo.scalaVersion}
          |Java ${System.getProperty("java.version")}.
          |System "${System.getProperty("os.name")}", version ${System.getProperty("os.version")}
       """.stripMargin)
  }
}
