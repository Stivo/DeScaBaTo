package ch.descabato

import org.rogach.scallop._
import scala.reflect.runtime.universe.TypeTag
import java.util.regex.Pattern
import java.math.{ BigDecimal => JBigDecimal }
import javax.xml.bind.DatatypeConverter
import java.text.DecimalFormat
import com.typesafe.scalalogging.slf4j.Logging
import java.io.PrintStream
import scala.collection.mutable.Buffer
import java.io.File
import ScallopConverters._
import java.io.BufferedReader
import java.io.InputStreamReader
import java.lang.reflect.InvocationTargetException

object CLI extends Utils {

  var testMode: Boolean = false

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
    case None => println("No command named " + name + " exists."); new HelpCommand()
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
    try {
      if (runsInJar) {
        java.lang.System.setOut(new PrintStream(System.out, true, "UTF-8"))
        parseCommandLine(args)
      } else {
        //      parseCommandLine("backup --serializer-type json --compression none backups ..\\testdata".split(" "))
        parseCommandLine("verify e:\\backups\\pics".split(" "))
        //      parseCommandLine("restore --help".split(" "))
        //      parseCommandLine("browse -p asdf backups".split(" "))
        //      parseCommandLine("restore --restore-to-folder restore --relative-to-folder . backups".split(" "))
      }
    } catch {
      case e @ PasswordWrongException(m, cause) =>
        println(m); logException(e)
      case e @ BackupVerification.BackupDoesntExist => println(e.getMessage); logException(e)
    }
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

trait BackupRelatedCommand extends Command {
  type T <: BackupFolderOption

  def newT(args: Seq[String]): T

  def needsExistingBackup = true

  final override def execute(args: Seq[String]) {
    start(newT(args))
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

trait ChangeableBackupOptions extends BackupFolderOption {
  val keylength = opt[Int](default = Some(128))
  val volumeSize = opt[Size](default = Some(Size("100Mb")))
  val threads = opt[Int](default = Some(1))
  val noScriptCreation = opt[Boolean](default = Some(false))
  val checkpointEvery = opt[Size](default = Some(Size("100Mb")))
}

trait CreateBackupOptions extends ChangeableBackupOptions {
  val serializerType = opt[String]()
  val compression = opt[CompressionMode]()
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
  val backupDestination = trailArg[String](required = true).map(new File(_).getAbsoluteFile())
}

class SubCommandBase(name: String) extends Subcommand(name) with BackupFolderOption

class BackupCommand extends BackupRelatedCommand {

  val suffix = if (Utils.isWindows) ".bat" else ""

  def writeBat(t: T, conf: BackupFolderConfiguration) = {
    val path = new File(s"descabato$suffix").getAbsolutePath()
    val prefix = if (conf.prefix == "") "" else "--prefix " + conf.prefix
    val line = s"$path backup $prefix ${t.backupDestination()} ${t.folderToBackup()}"
    def writeTo(bat: File) {
      if (!bat.exists) {
        val ps = new PrintStream(new Streams.UnclosedFileOutputStream(bat))
        ps.print(line)
        ps.close()
        println("A file " + bat + " has been written to execute this backup again")
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
  }

  override def needsExistingBackup = false
}

class RestoreCommand extends BackupRelatedCommand {
  type T = RestoreConf
  def newT(args: Seq[String]) = new RestoreConf(args)
  def start(t: T, conf: BackupFolderConfiguration) {
    println(t.summary)
    val rh = new RestoreHandler(conf) with ZipBlockStrategy
    if (t.chooseDate()) {
      val fm = new FileManager(conf)
      val options = fm.getBackupDates.zipWithIndex
      options.foreach { case (date, num) => println(s"[$num]: $date") }
      val option = askUser("Which backup would you like to restore from?").toInt
      rh.restoreFromDate(t, options.find(_._2 == option).get._1)
    } else {
      rh.restore(t)
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
    if (!CLI.testMode) {
      System.exit(count.toInt)
    }
  }
}

class SimpleBackupFolderOption(args: Seq[String]) extends ScallopConf(args) with BackupFolderOption

class BackupConf(args: Seq[String]) extends ScallopConf(args) with CreateBackupOptions {
  val folderToBackup = trailArg[File]().map(_.getAbsoluteFile())
}

class RestoreConf(args: Seq[String]) extends ScallopConf(args) with BackupFolderOption {
  val restoreToOriginalPath = opt[Boolean]()
  val chooseDate = opt[Boolean]()
  val restoreToFolder = opt[String]()
  val relativeToFolder = opt[String]()
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

// Domain classes
case class Size(bytes: Long) {
  override def toString = Utils.readableFileSize(bytes)
}

object Size {
  val knownTypes: Set[Class[_]] = Set(classOf[Size])
  val patt = Pattern.compile("([\\d.]+)[\\s]*([GMK]?B)", Pattern.CASE_INSENSITIVE);

  def apply(size: String): Size = {
    var out: Long = -1;
    val matcher = patt.matcher(size);
    val map = List(("GB", 3), ("MB", 2), ("KB", 1), ("B", 0)).toMap
    if (matcher.find()) {
      val number = matcher.group(1);
      val pow = map.get(matcher.group(2).toUpperCase()).get;
      var bytes = new BigDecimal(new JBigDecimal(number));
      bytes = bytes.*(BigDecimal.valueOf(1024).pow(pow));
      out = bytes.longValue();
    }
    new Size(out);
  }
}

object Utils {

  private val units = Array[String]("B", "KB", "MB", "GB", "TB");
  def isWindows = System.getProperty("os.name").contains("indows")
  def readableFileSize(size: Long, afterDot: Int = 1): String = {
    if (size <= 0) return "0";
    val digitGroups = (Math.log10(size) / Math.log10(1024)).toInt;
    val afterDotPart = if (afterDot == 0) "#" else "0" * afterDot
    return new DecimalFormat("#,##0." + afterDotPart).format(size / Math.pow(1024, digitGroups)) + Utils.units(digitGroups);
  }

  def encodeBase64(bytes: Array[Byte]) = DatatypeConverter.printBase64Binary(bytes);
  def decodeBase64(s: String) = DatatypeConverter.parseBase64Binary(s);

  def encodeBase64Url(bytes: Array[Byte]) = encodeBase64(bytes).replace('+', '-').replace('/', '_')
  def decodeBase64Url(s: String) = decodeBase64(s.replace('-', '+').replace('_', '/'));

  def normalizePath(x: String) = x.replace('\\', '/')

}

trait Utils extends Logging {
  lazy val l = logger
  import Utils._

  def readableFileSize(size: Long): String = Utils.readableFileSize(size)

  def logException(t: Throwable) {
    ObjectPools.baosPool.withObject(Unit, { baos =>
      val ps = new PrintStream(baos)
      def print(t: Throwable) {
        t.printStackTrace(ps)
        if (t.getCause() != null) {
          ps.println()
          ps.println("Caused by: ")
          print(t.getCause())
        }
      }
      print(t)
      l.debug(new String(baos.toByteArray))
    })
  }

}
