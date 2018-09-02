package ch.descabato.frontend

import java.io.{File, FileOutputStream, PrintStream}
import java.lang.reflect.InvocationTargetException
import java.nio.file.FileSystems

import ch.descabato.core.commands.{DoBackup, DoRestore, DoVerify}
import ch.descabato.core.config.{BackupConfigurationHandler, BackupFolderConfiguration}
import ch.descabato.core.model.Size
import ch.descabato.core.{BackupCorruptedException, BackupException, MisconfigurationException, Universe}
import ch.descabato.frontend.ScallopConverters._
import ch.descabato.utils.Implicits._
import ch.descabato.utils.Utils
import ch.descabato.version.BuildInfo
import ch.descabato.{CompressionMode, HashAlgorithm, RemoteMode}
import org.rogach.scallop._

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

  def withUniverse[T](conf: BackupFolderConfiguration)(f: Universe => T): T = {
    var universe: Universe = null
    try {
      universe = new Universe(conf)
      f(universe)
    } catch {
      case e: Exception =>
        logger.error("Exception while backing up")
        logException(e)
        throw e
    } finally {
      if (universe != null) {
        universe.shutdown()
      }
    }
  }

  final override def execute(args: Seq[String]) {
    try {
      lastArgs = args
      val t = newT(args)
      t.appendDefaultToDescription = true
      t.banner(s"DeScaBaTo ${BuildInfo.version}")
      t.verify()
      if (t.logfile.isSupplied) {
        validateFilename(t.logfile)
        System.setProperty("logname", t.logfile())
      }
      if (t.noAnsi.isSupplied) {
        AnsiUtil.ansiDisabled = t.noAnsi()
      }
      start(t)
    } catch {
      case e@MisconfigurationException(message) =>
        l.info(message)
        logException(e)
    }
  }

  def start(t: T) {
    import ch.descabato.core.config.BackupVerification._
    val confHandler = new BackupConfigurationHandler(t, needsExistingBackup)
    confHandler.verify() match {
      case b@BackupDoesntExist => throw b
      case PasswordNeeded =>
        val passphrase = askUser("This backup is passphrase protected. Please type your passphrase.", mask = true)
        confHandler.setPassphrase(passphrase)
      case OK =>
    }
    val conf = confHandler.updateAndGetConfiguration()
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

trait ChangeableBackupOptions extends BackupFolderOption with RedundancyOptions {
  val noGui: ScallopOption[Boolean] = opt[Boolean](noshort = true, descr = "Disables the user interface")
  val keylength: ScallopOption[Int] = opt[Int](descr = "Length of the AES encryption key", default = Some(128))
  val volumeSize: ScallopOption[Size] = opt[Size](descr = "Maximum size of the main data files", default = Some(Size("500Mb")))
  val threads: ScallopOption[Int] = opt[Int](descr = "How many threads should be used for the backup", default = Some(4))
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
  val noAnsi: ScallopOption[Boolean] = opt[Boolean](descr = "Disables Ansi features", hidden = true)
}

trait BackupFolderOption extends ProgramOption {
  val passphrase: ScallopOption[String] = opt[String](descr = "The password to use for the backup. If none is supplied, encryption is turned off", default = None)
  val backupDestination: ScallopOption[File] = trailArg[String](descr = "Root folder of the backup", required = true).map(new File(_).getCanonicalFile())
}

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
    if (t.noGui.isSupplied && t.noGui()) {
      ProgressReporters.guiEnabled = false
    }
    printConfiguration(t)
    withUniverse(conf) { universe =>
      if (!t.noScriptCreation()) {
        writeBat(t, conf, lastArgs)
      }
      val backup = new DoBackup(universe, t.folderToBackup() :: Nil)
      backup.execute()
    }
  }

  override def needsExistingBackup = false
}

object RestoreRunners extends Utils {

  def run(conf: BackupFolderConfiguration)(f: () => Unit) {
    while (true) {
      try {
        f()
        return
      } catch {
        case e@BackupCorruptedException(file, false) =>
          logException(e)
        //          Par2Handler.tryRepair(f, conf)
      }
    }
  }
}

class RestoreCommand extends BackupRelatedCommand {

  type T = RestoreConf

  def newT(args: Seq[String]) = new RestoreConf(args)

  def start(t: T, conf: BackupFolderConfiguration) {
    printConfiguration(t)
    validateFilename(t.restoreToFolder)
    validateFilename(t.restoreInfo)
    withUniverse(conf) {
      universe =>
        val fm = universe.fileManagerNew
        val restore = new DoRestore(universe)
        if (t.chooseDate()) {
          val options = fm.backup.getFiles().map(fm.backup.dateOfFile).zipWithIndex
          options.foreach {
            case (date, num) => println(s"[$num]: $date")
          }
          val option = askUser("Which backup would you like to restore from?").toInt
          restore.restoreFromDate(t, options.find(_._2 == option).get._1)
        } else if (t.restoreBackup.isSupplied) {
          val backupsFound = fm.backup.getFiles().filter(_.getName.equals(t.restoreBackup()))
          if (backupsFound.isEmpty) {
            println("Could not find described backup, these are your choices:")
            fm.backup.getFiles().foreach { f =>
              println(f)
            }
            throw new IllegalArgumentException("Backup not found")
          }
          restore.restoreFromDate(t, fm.backup.dateOfFile(backupsFound.head))

        } else {
          restore.restore(t)
        }
    }
  }
}

class VerifyCommand extends BackupRelatedCommand {
  type T = VerifyConf

  def newT(args: Seq[String]) = new VerifyConf(args)

  def start(t: T, conf: BackupFolderConfiguration): Unit = {
    printConfiguration(t)
    val counter = withUniverse(conf) {
      u =>
        val verify = new DoVerify(u)
        verify.verifyAll()
    }
    CLI.lastErrors = counter.count
    if (counter.count != 0)
      CLI.exit(counter.count.toInt)
  }
}

class SimpleBackupFolderOption(args: Seq[String]) extends ScallopConf(args) with BackupFolderOption

class BackupConf(args: Seq[String]) extends ScallopConf(args) with CreateBackupOptions {
  val folderToBackup: ScallopOption[File] = trailArg[File](descr = "Folder to be backed up").map(_.getCanonicalFile())
}

class RestoreConf(args: Seq[String]) extends ScallopConf(args) with BackupFolderOption {
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

class VerifyConf(args: Seq[String]) extends ScallopConf(args) with BackupFolderOption {
  val percentOfFilesToCheck: ScallopOption[Int] = opt[Int](default = Some(5), descr = "How many percent of files to check")
  validate(percentOfFilesToCheck) {
    case x if x > 0 && x <= 100 => Right(Unit)
    case _ => Left("Needs to be percent")
  }
}

class HelpCommand extends Command {
  val T = ScallopConf

  override def execute(args: Seq[String]) {
    args.toList match {
      case command :: _ if CLI.getCommands().safeContains(command) => CLI.parseCommandLine(command :: "--help" :: Nil)
      case _ =>
        val commands = CLI.getCommands().keys.mkString(", ")
        println(
          s"""Welcome to DeScaBaTo.
The available commands are: $commands
For further help about a specific command type 'help backup' or 'backup --help'.
For general usage guide go to https://github.com/Stivo/DeScaBaTo""")
    }
  }
}


class VersionCommand extends Command {
  val T = ScallopConf

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

