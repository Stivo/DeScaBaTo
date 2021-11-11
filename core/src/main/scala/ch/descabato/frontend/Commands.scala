package ch.descabato.frontend

import ch.descabato.CompressionMode
import ch.descabato.HashAlgorithm
import ch.descabato.Main
import ch.descabato.RemoteMode
import ch.descabato.core.BackupException
import ch.descabato.core.ExceptionFactory
import ch.descabato.core.MisconfigurationException
import ch.descabato.core.config.BackupConfigurationHandler
import ch.descabato.core.config.BackupFolderConfiguration
import ch.descabato.core.model.Size
import ch.descabato.core.util.FileManager
import ch.descabato.frontend.ScallopConverters._
import ch.descabato.utils.BuildInfo
import ch.descabato.utils.Implicits._
import ch.descabato.utils.Utils
import org.rogach.scallop._
import org.rogach.scallop.exceptions.ScallopException

import java.io.File
import java.lang.reflect.InvocationTargetException
import java.nio.file.FileSystems
import scala.reflect.runtime.universe
import scala.reflect.runtime.universe.TypeTag

object ScallopConverters {
  def singleArgConverter[A](conv: String => A, msg: String = "wrong arguments format")(implicit tt: TypeTag[A]): ValueConverter[A] = new ValueConverter[A] {
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

    val tag: universe.TypeTag[A] = tt
    val argType: ArgType.V = ArgType.SINGLE
  }

  implicit val modeConverter: ValueConverter[CompressionMode] = singleArgConverter[CompressionMode](CompressionMode.valueOf, "Should be one of " + CompressionMode.values.mkString(", "))
  implicit val hashAlgorithmConverter: ValueConverter[HashAlgorithm] = singleArgConverter[HashAlgorithm](HashAlgorithm.valueOf, "Should be one of " + HashAlgorithm.values.mkString(", "))
  implicit val remoteModeConverter: ValueConverter[RemoteMode] = singleArgConverter[RemoteMode](RemoteMode.fromCli, RemoteMode.message)
  implicit val sizeConverter: ValueConverter[Size] = singleArgConverter[Size](x => Size(x))

}

trait Command {

  def name: String = this.getClass().getSimpleName().replace("Command", "").toLowerCase()

  def execute(args: Seq[String])

  def printConfiguration[T <: BackupFolderOption](t: T): Unit = {
    if (t.passphrase.isDefined) {
      println(t.filteredSummary(Set(t.passphrase.name)))
    } else {
      println(t.summary)
    }
  }

  def askUser(question: String = "Do you want to continue?", mask: Boolean = false): String = {
    println(question)
    if (mask)
      System.console().readPassword().mkString
    else
      System.console().readLine()
  }

  def askUserYesNo(question: String = "Do you want to continue?"): Boolean = {
    val answer = askUser(question)
    val yes = Set("yes", "y")
    if (yes.contains(answer.toLowerCase().trim)) {
      true
    } else {
      println("User aborted")
      false
    }
  }

}

class ReflectionCommand(override val name: String, clas: String) extends Command {

  def execute(args: Seq[String]) {
    try {
      val clazz = Class.forName(clas)
      val instance = clazz.getConstructor().newInstance()
      clazz.getMethod("execute", classOf[Seq[String]]).invoke(instance, args)
    } catch {
      case e: ReflectiveOperationException if e.getCause().isInstanceOf[BackupException] => throw e.getCause
      case e: InvocationTargetException if e.getCause() != null => throw e.getCause
    }
  }

}

trait BackupRelatedCommand extends Command with Utils {
  type T <: BackupFolderOption

  def newT(args: Seq[String]): T

  def needsExistingBackup = true

  var lastArgs: Seq[String] = Nil

  final override def execute(args: Seq[String]) {
    val t = newT(args)
    try {
      t.appendDefaultToDescription = true
      lastArgs = args
      t.banner(s"DeScaBaTo ${BuildInfo.version}")
      t.verify()
      if (t.logfile.isSupplied) {
        validateFilename(t.logfile)
        System.setProperty("logname", t.logfile())
        println(s"Log name set to ${t.logfile()}")
      }
      t match {
        case ng: NoGuiOption =>
          if (ng.noGui.isSupplied && ng.noGui()) {
            ProgressReporters.guiEnabled = false
          }
        case _ => // pass
      }
      start(t)
    } catch {
      case e@MisconfigurationException(message) =>
        l.info(message)
        logException(e)
      case e: ScallopException =>
        l.info(e.message)
        l.info("Help for command:")
        t.printHelp()
    }
  }

  def checkForUpgradeNeeded = true

  def overrideVersion: Option[String] = None

  def start(t: T) {
    import ch.descabato.core.config.BackupVerification._
    val confHandler = new BackupConfigurationHandler(t, needsExistingBackup)
    confHandler.verify() match {
      case b@BackupDoesntExist => throw BackupDoesntExist
      case PasswordNeeded =>
        val passphrase = askUser("This backup is passphrase protected. Please type your passphrase.", mask = true)
        confHandler.setPassphrase(passphrase)
      case OK =>
    }
    val conf = confHandler.updateAndGetConfiguration()
    val manager = new FileManager(conf)
    if (checkForUpgradeNeeded && (manager.metadata.getFiles().nonEmpty || manager.volumeIndex.getFiles().nonEmpty || manager.backup.getFiles().nonEmpty)) {
      throw ExceptionFactory.createUpgradeException(conf.version)
    }
    start(t, conf)
  }

  def start(t: T, conf: BackupFolderConfiguration)

  def validateFilename(option: ScallopOption[String]) {
    if (option.isDefined) {
      val s = option()
      try {
        // validate the restore to folder, as this will throw an exception
        FileSystems.getDefault().getPath(s)
      } catch {
        case e: Exception =>
          l.error(s"$s for ${option.name} is not a valid filename: ${e.getMessage}")
          System.exit(1)
      }
    }
  }
}

// Parsing classes

trait RedundancyOptions extends BackupFolderOption {
  //  val metadataRedundancy = opt[Int](default = Some(20))
  //  val volumeRedundancy = opt[Int](default = Some(5))
  //  val noRedundancy = opt[Boolean](default = Some(false))
}

trait ChangeableBackupOptions extends BackupFolderOption with RedundancyOptions with NoGuiOption {
  val keylength: ScallopOption[Int] = opt[Int](descr = "Length of the AES encryption key", default = Some(128))
  val volumeSize: ScallopOption[Size] = opt[Size](descr = "Maximum size of the main data files", default = Some(Size("500Mb")))
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
  val logfile: ScallopOption[String] = opt[String](descr = "Destination of the logfile of this backup")
}

trait NoGuiOption extends ScallopConf {
  val noGui: ScallopOption[Boolean] = opt[Boolean](noshort = true, descr = "Disables the progress report window")
}

trait BackupFolderOption extends ProgramOption {
  val passphrase: ScallopOption[String] = opt[String](descr = "The password to use for the backup. If none is supplied, encryption is turned off", default = None)
  val backupDestination: ScallopOption[File] = trailArg[String](descr = "Root folder of the backup", required = true).map(new File(_).getCanonicalFile())

}

class SimpleBackupFolderOption(args: Seq[String]) extends ScallopConf(args) with BackupFolderOption

class BackupConf(args: Seq[String]) extends ScallopConf(args) with CreateBackupOptions {
  val folderToBackup: ScallopOption[File] = trailArg[File](descr = "Folder to be backed up").map(_.getCanonicalFile())
}

class CountConf(args: Seq[String]) extends ScallopConf(args) with ProgramOption {
  val dontSaveSymlinks: ScallopOption[Boolean] = opt[Boolean](default = Some(false), descr = "Disable backing up symlinks")
  val ignoreFile: ScallopOption[File] = opt[File](descr = "File with ignore patterns", default = None)
  val folderToCountIn: ScallopOption[File] = trailArg[File](descr = "Folder to count files in").map(_.getCanonicalFile())
}

class MultipleBackupConf(args: Seq[String]) extends ScallopConf(args) with CreateBackupOptions {
  val foldersToBackup: ScallopOption[List[File]] = trailArg[List[File]](descr = "Folders to be backed up").map(_.map(_.getCanonicalFile()))
}

class RestoreConf(args: Seq[String]) extends ScallopConf(args) with BackupFolderOption with NoGuiOption {
  val restoreToOriginalPath: ScallopOption[Boolean] = opt[Boolean](descr = "Restore files to original path.")
  val chooseDate: ScallopOption[Boolean] = opt[Boolean](descr = "Choose the date you want to restore from a list.")
  val restoreToFolder: ScallopOption[String] = opt[String](descr = "Restore to a given folder")
  val restoreBackup: ScallopOption[String] = opt[String](descr = "Filename of the backup to restore.")
  val restoreInfo: ScallopOption[String] = opt[String](descr = "Destination of a short summary of the restore process.")
  //  val overwriteExisting = toggle(default = Some(false))
  //  val pattern = opt[String]()
  requireOne(restoreToOriginalPath, restoreToFolder)
  mutuallyExclusive(restoreBackup, chooseDate)
}

class VerifyConf(args: Seq[String]) extends ScallopConf(args) with BackupFolderOption with NoGuiOption {
  val percentOfFilesToCheck: ScallopOption[Int] = opt[Int](default = Some(5), descr = "How many percent of files to check")
  val checkFirstOfEachVolume: ScallopOption[Boolean] = opt[Boolean](default = Some(false), descr = "Check whether the first entry of each volume is correct or not")
  validate(percentOfFilesToCheck) {
    case x if x >= 0 && x <= 100 => Right(())
    case _ => Left("Needs to be percent")
  }
}

class HelpCommand extends Command {

  override def execute(args: Seq[String]) {
    args.toList match {
      case command :: _ if Main.getCommands().safeContains(command) => Main.parseCommandLine(command :: "--help" :: Nil)
      case _ =>
        val commands = Main.getCommands().keys.mkString(", ")
        println(
          s"""|Welcome to DeScaBaTo ${BuildInfo.version}.
              |The available commands are: $commands
              |For further help about a specific command type 'help backup' or 'backup --help'.
              |For general usage guide go to https://github.com/Stivo/DeScaBaTo""".stripMargin
        )
    }
  }
}

class VersionCommand extends Command {

  override def name: String = "--version"

  override def execute(args: Seq[String]) {
    println(
      s"""|DeScaBaTo version ${BuildInfo.version}
          |Scala ${BuildInfo.scalaVersion}
          |Java ${System.getProperty("java.version")}.
          |System "${System.getProperty("os.name")}", version ${System.getProperty("os.version")}
       """.stripMargin)
  }
}
