package ch.descabato.frontend

import java.io.File
import java.io.FileOutputStream
import java.io.PrintStream
import java.lang.reflect.InvocationTargetException
import java.security.Security

import ch.descabato.CompressionMode
import ch.descabato.core._
import ch.descabato.frontend.ScallopConverters._
import ch.descabato.utils.Implicits._
import ch.descabato.utils.Utils
import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.rogach.scallop._

import scala.reflect.runtime.universe.TypeTag

object CLI extends Utils {

  var paused: Boolean = false

  var lastErrors: Long = 0L

  var _overrideRunsInJar = false

  def runsInJar = _overrideRunsInJar || classOf[CreateBackupOptions].getResource("CreateBackupOptions.class").toString.startsWith("jar:")

  def getCommands() = List(
    new BackupCommand(),
    new VerifyCommand(),
    new RestoreCommand(),
    new ReflectionCommand("browse", "ch.descabato.browser.BrowseCommand"),
    new HelpCommand()).map(x => (x.name.toLowerCase, x)).toMap

  def getCommand(name: String): Command = getCommands.get(name.toLowerCase()) match {
    case Some(x) => x
    case None => l.warn("No command named " + name + " exists."); new HelpCommand()
  }

  def parseCommandLine(args: Seq[String]) {
    val (command, tail) = if (args.isEmpty) {
      ("help", Nil)
    } else {
      (args.head, args.tail)
    }
    getCommand(command).execute(tail)
  }

  def main(args: Array[String]) {
    if (System.getProperty("logname") == null)
      System.setProperty("logname", "backup.log")
    try {
      Security.addProvider(new BouncyCastleProvider())
      if (runsInJar) {
        java.lang.System.setOut(new PrintStream(System.out, true, "UTF-8"))
        parseCommandLine(args)
        exit(0)
      } else {
        //Thread.sleep(10000)
        try {
//          FileUtils.deleteAll(new File("f:/desca8"))
        } catch {
          case x: Exception =>
        }
//        parseCommandLine("backup --threads 1 --serializer-type json --hash-algorithm md5 --compression none --volume-size 10mb C:/Users/Stivo/workspace-luna/DeScaBaTo/integrationtest/testdata/backup1 C:/Users/Stivo/workspace-luna/DeScaBaTo/integrationtest/testdata/input".split(" "))
        //parseCommandLine("restore --restore-to-folder H:/testdata/restore1 H:/testdata/backup1".split(" "))
        //parseCommandLine("restore --restore-to-folder C:/Users/Stivo/workspace-luna/DeScaBaTo/integrationtest/testdata/restore1 C:/Users/Stivo/workspace-luna/DeScaBaTo/integrationtest/testdata/backup1".split(" "))
        //        parseCommandLine("backup --serializer-type json --hash-algorithm sha-512 --compression gzip --volume-size 50mb f:/desca8 d:/pics/tosort/bilder".split(" "))
        // parseCommandLine("backup --serializer-type json --volume-size 5mb backups ..\\testdata".split(" "))
        //parseCommandLine("backup --serializer-type json --hash-algorithm sha-256 --compression gzip --volume-size 100mb e:/temp/desca9 d:/pics/tosort".split(" "))

//        parseCommandLine("backup --passphrase testasdf --threads 10 --serializer-type json --compression lz4 --volume-size 100mb L:/desca8 L:/tmp".split(" "))

//        parseCommandLine("verify --passphrase testasdf --percent-of-files-to-check 100 L:\\desca8".split(" "))
//        parseCommandLine("restore --restore-to-folder F:/restore f:/desca8".split(" "))
        // parseCommandLine("backup --no-redundancy --serializer-type json --compression none --volume-size 5mb backups /home/stivo/progs/eclipse-fresh".split(" "))
        //        parseCommandLine("verify e:\\backups\\pics".split(" "))
        //              parseCommandLine("restore --help".split(" "))
                      parseCommandLine("browse -p testasdf L:/desca8".split(" "))
        //        parseCommandLine("restore --passphrase mypasshere --restore-to-folder restore --relative-to-folder . backups".split(" "))
        exit(0)
      }
    } catch {
      case e@PasswordWrongException(m, cause) =>
        l.warn(m)
        logException(e)
        exit(-1)
      case e@BackupVerification.BackupDoesntExist =>
        l.warn(e.getMessage)
        logException(e)
        exit(-2)
      case e: Error =>
        l.warn(e.getMessage)
        logException(e)
        exit(-3)
    }
  }

  def exit(exitCode: Int) {
    System.exit(exitCode)
  }

}

object ScallopConverters {
  def singleArgConverter[A](conv: String => A, msg: String = "wrong arguments format")(implicit tt: TypeTag[A]) = new ValueConverter[A] {
    def parse(s: List[(String, List[String])]) = {
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

    val tag = tt
    val argType = ArgType.SINGLE
  }

  implicit val modeConverter = singleArgConverter[CompressionMode](CompressionMode.valueOf, "Should be one of " + CompressionMode.values.mkString(", "))
  implicit val sizeConverter = singleArgConverter[Size](x => Size(x))

}

trait Command {

  def name = this.getClass().getSimpleName().replace("Command", "").toLowerCase()

  def execute(args: Seq[String])

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

  def withUniverse[T](conf: BackupFolderConfiguration, akkaAllowed: Boolean = true)(f: Universe => T) = {
    var universe: Universe = null
    try {
      universe = if (akkaAllowed)
        Universes.makeUniverse(conf)
      else
        new SingleThreadUniverse(conf)
      f(universe)
    } catch {
      case e: Exception =>
        logger.error("Exception while backing up")
        logException(e)      
        throw e
    } finally {
      if (universe != null)
        universe.shutdown
    }
  }

  final override def execute(args: Seq[String]) {
    try {
      lastArgs = args
      val t = newT(args)
      t.afterInit()
      if (t.noGui.isSupplied && t.noGui()) {
        ProgressReporters.guiEnabled = false
      }
      if (t.logfile.isSupplied) {
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
    import ch.descabato.core.BackupVerification._
    val confHandler = new BackupConfigurationHandler(t)
    var passphrase = t.passphrase.get
    confHandler.verify(needsExistingBackup) match {
      case b@BackupDoesntExist => throw b
      case PasswordNeeded => passphrase = Some(askUser("This backup is passphrase protected. Please type your passphrase.", mask = true))
      case OK =>
    }
    val conf = confHandler.configure(passphrase)
    start(t, conf)
  }

  def start(t: T, conf: BackupFolderConfiguration)

}

// Parsing classes

trait RedundancyOptions extends BackupFolderOption {
//  val metadataRedundancy = opt[Int](default = Some(20))
//  val volumeRedundancy = opt[Int](default = Some(5))
//  val noRedundancy = opt[Boolean](default = Some(false))
}

trait ChangeableBackupOptions extends BackupFolderOption with RedundancyOptions {
  val keylength = opt[Int](default = Some(128))
  val volumeSize = opt[Size](default = Some(Size("100Mb")))
  val threads = opt[Int](default = Some(2))
  val noScriptCreation = opt[Boolean](default = Some(false))
//  val renameDetection = opt[Boolean](hidden = true, default = Some(false))
  val dontSaveSymlinks = opt[Boolean](default = Some(false))
  val compression = opt[CompressionMode](default = Some(CompressionMode.smart))
  val createIndexes = opt[Boolean](default = Some(false))
}

trait CreateBackupOptions extends ChangeableBackupOptions {
  val serializerType = opt[String]()
  val blockSize = opt[Size](default = Some(Size("100Kb")))
  val hashAlgorithm = opt[String](default = Some("md5"))
}

trait ProgramOption extends ScallopConf {
  val noGui = opt[Boolean](short = 'g')
  val logfile = opt[String]()
  val noAnsi = opt[Boolean]()
}

trait BackupFolderOption extends ProgramOption {
  val passphrase = opt[String](default = None)
  val prefix = opt[String](default = Some("")).map {
    x =>
      if (x == "") {
        x
      } else {
        x.last match {
          case '.' | '_' | '-' | ';' => x
          case _ => x + "_"
        }
      }
  }
  val backupDestination = trailArg[String](required = true).map(new File(_).getCanonicalFile())
}

class SubCommandBase(name: String) extends Subcommand(name) with BackupFolderOption

class BackupCommand extends BackupRelatedCommand with Utils {

  val suffix = if (Utils.isWindows) ".bat" else ""

  def writeBat(t: T, conf: BackupFolderConfiguration, args: Seq[String]) = {
    var path = new File(s"${conf.prefix}descabato$suffix").getCanonicalFile
    // TODO the directory should be determined by looking at the classpath
    if (!path.exists) {
      path = new File(path.getParent() + "/bin", path.getName)
    }
    val line = s"$path backup "+args.mkString(" ")
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
    println(t.summary)
    withUniverse(conf) {
      universe =>
        val bdh = new BackupHandler(universe)
        if (!t.noScriptCreation()) {
          writeBat(t, conf, lastArgs)
        }
        bdh.backup(t.folderToBackup() :: Nil)
    }
    //    if (conf.redundancyEnabled) {
    //      l.info("Running redundancy creation")
    //      new RedundancyHandler(conf).createFiles
    //      l.info("Redundancy creation finished")
    //    }
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
    println(t.summary)
    withUniverse(conf, akkaAllowed = false) {
      universe =>
        if (t.chooseDate()) {
          val fm = new FileManager(universe)
          val options = fm.getBackupDates.zipWithIndex
          options.foreach {
            case (date, num) => println(s"[$num]: $date")
          }
          val option = askUser("Which backup would you like to restore from?").toInt
          RestoreRunners.run(conf) { () =>
              val rh = new RestoreHandler(universe)
              rh.restoreFromDate(t, options.find(_._2 == option).get._1)
          }
        } else {
          RestoreRunners.run(conf) { () =>
              val rh = new RestoreHandler(universe)
              rh.restore(t)
          }
        }
    }
  }
}

class VerifyCommand extends BackupRelatedCommand {
  type T = VerifyConf

  def newT(args: Seq[String]) = new VerifyConf(args)

  def start(t: T, conf: BackupFolderConfiguration) = {
    println(t.summary)
    val count = withUniverse(conf, akkaAllowed = false) {
      u =>
        val rh = new VerifyHandler(u)
        rh.verify(t)
    }
    CLI.lastErrors = count
    if (count != 0)
      CLI.exit(count.toInt)
  }
}

class SimpleBackupFolderOption(args: Seq[String]) extends ScallopConf(args) with BackupFolderOption

class BackupConf(args: Seq[String]) extends ScallopConf(args) with CreateBackupOptions {
  val folderToBackup = trailArg[File]().map(_.getCanonicalFile())
}

class RestoreConf(args: Seq[String]) extends ScallopConf(args) with BackupFolderOption {
  val restoreToOriginalPath = opt[Boolean]()
  val chooseDate = opt[Boolean]()
  val restoreToFolder = opt[String]()
  //  val overwriteExisting = toggle(default = Some(false))
//  val pattern = opt[String]()
  requireOne(restoreToOriginalPath, restoreToFolder)
}

class VerifyConf(args: Seq[String]) extends ScallopConf(args) with BackupFolderOption {
  val percentOfFilesToCheck = opt[Int](default = Some(5))
  validate(percentOfFilesToCheck) {
    xIn: Int =>
      xIn match {
        case x if x > 0 && x <= 100 => Right(Unit)
        case _ => Left("Needs to be percent")
      }
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
          s"""The available commands are: $commands
For further help about a specific command type 'help backup' or 'backup --help'.
For general usage guide go to https://github.com/Stivo/DeScaBaTo""")
    }
  }
}

