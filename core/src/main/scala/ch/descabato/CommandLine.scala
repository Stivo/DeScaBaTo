package ch.descabato

import org.rogach.scallop._
import scala.reflect.runtime.universe.TypeTag
import java.util.regex.Pattern
import javax.xml.bind.DatatypeConverter
import com.typesafe.scalalogging.slf4j.Logging
import java.io.PrintStream
import scala.collection.mutable.Buffer
import java.io.File
import ScallopConverters._
import java.io.BufferedReader
import java.io.InputStreamReader
import java.lang.reflect.InvocationTargetException

object CLI extends Utils {

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
    System.setProperty("logname", "backup.log")  
    try {
      if (runsInJar) {
        java.lang.System.setOut(new PrintStream(System.out, true, "UTF-8"))
        parseCommandLine(args)
        exit(0)
      } else {
    	  parseCommandLine("backup --passphrase mypasshere --serializer-type json --compression none --volume-size 5mb backups ..\\testdata".split(" "))
        // parseCommandLine("backup --no-redundancy --serializer-type json --compression none --volume-size 5mb backups /home/stivo/progs/eclipse-fresh".split(" "))
        //        parseCommandLine("verify e:\\backups\\pics".split(" "))
        //              parseCommandLine("restore --help".split(" "))
        //              parseCommandLine("browse -p asdf backups".split(" "))
//        parseCommandLine("restore --passphrase mypasshere --restore-to-folder restore --relative-to-folder . backups".split(" "))
      }
    } catch {
      case e @ PasswordWrongException(m, cause) =>
        l.warn(m); logException(e)
        exit(-1)
      case e @ BackupVerification.BackupDoesntExist => 
        l.warn(e.getMessage); logException(e)
        exit(-2)
      case e : Error =>
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
          try { Right(Some(conv(i))) } catch { case _: Throwable => Left(msg) }
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
    }
  }

}

trait BackupRelatedCommand extends Command with Utils {
  type T <: BackupFolderOption

  def newT(args: Seq[String]): T

  def needsExistingBackup = true

  final override def execute(args: Seq[String]) {
    try {
      val t = newT(args)
      if (t.logfile.isSupplied) {
        System.setProperty("logname", t.logfile())
      }
      start(t)
    } catch {
      case e @ MisconfigurationException(message) =>
        l.info(message)
        logException(e)
    }
  }

  def start(t: T) {
    import BackupVerification._
    t.afterInit
    val confHandler = new BackupConfigurationHandler(t)
    var passphrase = t.passphrase.get
    confHandler.verify(needsExistingBackup) match {
      case b @ BackupDoesntExist => throw b
      case PasswordNeeded => passphrase = Some(askUser("This backup is passphrase protected. Please type your passphrase.", true))
      case OK =>
    }
    val conf = confHandler.configure(passphrase)
    start(t, conf)
  }

  def start(t: T, conf: BackupFolderConfiguration)

}

// Parsing classes

trait RedundancyOptions extends BackupFolderOption {
  val metadataRedundancy = opt[Int](default = Some(20))
  val volumeRedundancy = opt[Int](default = Some(5))
  val noRedundancy = opt[Boolean](default = Some(false))
}

trait ChangeableBackupOptions extends BackupFolderOption with RedundancyOptions {
  val keylength = opt[Int](default = Some(128))
  val volumeSize = opt[Size](default = Some(Size("100Mb")))
  val threads = opt[Int](default = Some(1))
  val noScriptCreation = opt[Boolean](default = Some(false))
  val checkpointEvery = opt[Size](default = Some(Size("100Mb")))
  val renameDetection = opt[Boolean](default = Some(false))
  val dontSaveSymlinks = opt[Boolean](default = Some(false))
  val compression = opt[CompressionMode]()
}

trait CreateBackupOptions extends ChangeableBackupOptions {
  val serializerType = opt[String]()
  val blockSize = opt[Size](default = Some(Size("100Kb")))
  val hashAlgorithm = opt[String](default = Some("md5"))
}

trait BackupFolderOption extends ScallopConf {
  val passphrase = opt[String](default = None)
  val prefix = opt[String](default = Some("")).map { x =>
    if (x == "") {
      x
    } else {
      x.last match {
        case '.' | '_' | '-' | ';' => x
        case _ => x + "_"
      }
    }
  }
  val logfile = opt[String]()
  val backupDestination = trailArg[String](required = true).map(new File(_).getCanonicalFile())
}

class SubCommandBase(name: String) extends Subcommand(name) with BackupFolderOption

class BackupCommand extends BackupRelatedCommand with Utils {

  val suffix = if (Utils.isWindows) ".bat" else ""

  def writeBat(t: T, conf: BackupFolderConfiguration) = {
    val path = new File(s"descabato$suffix").getCanonicalPath()
    val prefix = if (conf.prefix == "") "" else "--prefix " + conf.prefix
    val line = s"$path backup $prefix ${t.backupDestination()} ${t.folderToBackup()}"
    def writeTo(bat: File) {
      if (!bat.exists) {
        val ps = new PrintStream(new Streams.UnclosedFileOutputStream(bat))
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
    val bdh = new BackupHandler(conf) with ZipBlockStrategy
    if (!t.noScriptCreation()) {
      writeBat(t, conf)
    }
    bdh.backup(t.folderToBackup() :: Nil)
    if (conf.redundancyEnabled) {
      l.info("Running redundancy creation")
      new RedundancyHandler(conf).createFiles
      l.info("Redundancy creation finished")
    }
  }

  override def needsExistingBackup = false
}

object RestoreRunners extends Utils {

  def run(conf: BackupFolderConfiguration)(f: Unit => Unit) {
    while (true) {
      try {
        f()
        return
      } catch {
        case e @ BackupCorruptedException(f, false) =>
          logException(e)
          Par2Handler.tryRepair(f, conf)
      }
    }
  }
}

class RestoreCommand extends BackupRelatedCommand {
  type T = RestoreConf
  def newT(args: Seq[String]) = new RestoreConf(args)
  def start(t: T, conf: BackupFolderConfiguration) {
    println(t.summary)
    if (t.chooseDate()) {
      val fm = new FileManager(conf)
      val options = fm.getBackupDates.zipWithIndex
      options.foreach { case (date, num) => println(s"[$num]: $date") }
      val option = askUser("Which backup would you like to restore from?").toInt
      RestoreRunners.run(conf) { _ =>
        val rh = new RestoreHandler(conf) with ZipBlockStrategy
        rh.restoreFromDate(t, options.find(_._2 == option).get._1)
      }
    } else {
      RestoreRunners.run(conf) { _ =>
        val rh = new RestoreHandler(conf) with ZipBlockStrategy
        rh.restore(t)
      }
    }

  }
}

class VerifyCommand extends BackupRelatedCommand {
  type T = VerifyConf
  def newT(args: Seq[String]) = new VerifyConf(args)
  def start(t: T, conf: BackupFolderConfiguration) = {
    println(t.summary)
    val rh = new VerifyHandler(conf) with ZipBlockStrategy
    val count = rh.verify(t)
    CLI.lastErrors = count
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
  val pattern = opt[String]()
  mutuallyExclusive(restoreToOriginalPath, restoreToFolder)
}

class VerifyConf(args: Seq[String]) extends ScallopConf(args) with BackupFolderOption {
  val percentOfFilesToCheck = opt[Int](default = Some(5))
  validate(percentOfFilesToCheck) { x: Int =>
    x match {
      case x if x > 0 && x <= 100 => Right(Unit)
      case _ => Left("Needs to be percent")
    }
  }
}

class HelpCommand extends Command {
  val T = ScallopConf
  override def execute(args: Seq[String]) {
    args.toList match {
      case command :: _ if CLI.getCommands().contains(command) => CLI.parseCommandLine(command :: "--help" :: Nil)
      case _ =>
        val commands = CLI.getCommands().keys.mkString(", ")
        println(
          s"""The available commands are: $commands
For further help about a specific command type 'help backup' or 'backup --help'.
For general usage guide go to https://github.com/Stivo/DeScaBaTo""")
    }
  }
}

