package ch.descabato

import org.rogach.scallop._
import scala.reflect.runtime.universe.TypeTag
import java.util.regex.Pattern
import java.math.{ BigDecimal => JBigDecimal }
import javax.xml.bind.DatatypeConverter
import java.text.DecimalFormat
import backup.ConsoleManager
import com.typesafe.scalalogging.slf4j.Logging
import CLI._
import java.io.PrintStream
import scala.collection.mutable.Buffer
import java.io.File

object CLI {

  implicit val sizeConverter = singleArgConverter[Size](x => SizeParser.parse(x))

  def runsInJar = classOf[CreateBackupOptions].getResource("CreateBackupOptions.class").toString.startsWith("jar:")

  def getCommand(args: Seq[String]): Command = args.toList match {
    case "backup" :: args => new BackupCommand(args)
    case "restore" :: args => new RestoreCommand(args)
    //    case "newbackup" :: args => new NewBackupCommand(args)
    case _ => println("TODO"); throw new IllegalArgumentException("asdf") //new HelpCommand()
  }

  def parseCommandLine(args: Seq[String]) {
    getCommand(args).execute()
  }

  def main(args: Array[String]) {
    if (runsInJar) {
      java.lang.System.setOut(new PrintStream(System.out, true, "UTF-8"))
    } else {
      parseCommandLine("backup backups test".split(" "))
      parseCommandLine("restore --restore-to-folder restore --relative-to-folder . backups".split(" "))
    }
  }

}

trait Command {
  type T <: ScallopConf
  val args: Seq[String]
  def newT: T
  
  //  def name : String
  final def execute() {
    start(newT)
  }
  def start(t: T)
}

trait BackupRelatedCommand extends Command {
  type T <: BackupFolderOption
  def start(t: T) {
    t.builder.verify
     val conf = new BackupConfigurationHandler(newT).configure
     start(t, conf)
  }
  
  def start(t: T, conf: BackupFolderConfiguration)
  
}

// Parsing classes

trait CreateBackupOptions extends BackupFolderOption {
  val blockSize = opt[Size](default = Some(new Size("100Kb")))
  val hashAlgorithm = opt[String](default = Some("md5"))
}

trait BackupFolderOption extends ScallopConf {
  val passphrase = opt[String](default = None)
  val backupDestination = trailArg[String]()
}



class SubCommandBase(name: String) extends Subcommand(name) with BackupFolderOption

class BackupCommand(val args: Seq[String]) extends BackupRelatedCommand {
  type T = BackupConf
  val newT = new BackupConf(args)
  def start(t: T, conf: BackupFolderConfiguration) {
    t.builder.verify
    println(t.summary)
    val bdh = new BackupHandler(conf) with ZipBlockStrategy
    bdh.backup(t.folderToBackup() :: Nil)
  }
}

class RestoreCommand(val args: Seq[String]) extends BackupRelatedCommand {
  type T = RestoreConf
  val newT = new RestoreConf(args)
  def start(t: T, conf: BackupFolderConfiguration) {
    t.builder.verify
    println(t.summary)
    
    val rh = new RestoreHandler(conf) with ZipBlockStrategy
    rh.restore(t)
  }
}

class BackupConf(args: Seq[String]) extends ScallopConf(args) with BackupFolderOption {
  val folderToBackup = trailArg[String]().map(x => new File(x))
  //lazy val folderToBackupFiles = List(folderToBackup).map(x => new File(x()))
}

class RestoreConf(args: Seq[String]) extends ScallopConf(args) with BackupFolderOption {
  val restoreToOriginalPath = toggle()
  val restoreToFolder = opt[String]( )
  val relativeToFolder = opt[String]( )
//  val overwriteExisting = toggle(default = Some(false))
  val pattern = opt[String]()
  mutuallyExclusive(restoreToOriginalPath, restoreToFolder)
}


//class HelpCommand extends Command wit{
//    def execute(t: T) : Unit {}
//}

class BackupCommands(args: Seq[String]) extends ScallopConf(args) {
  def this(args: String) = this(args.split(" "))

  //implicit val modeConverter = singleArgConverter[CompressionMode](CompressionMode.valueOf)

  val backup = new Subcommand("backup") with BackupFolderOption {
    //val mode = opt[CompressionMode]()
    val folderToBackup = trailArg[File]()

    override def toString() = "This is backup, hi there"
  }
  val restore = new Subcommand("restore") with BackupFolderOption {
    val restoreToOriginal = toggle(default = Some(false))
  }
  val browse = new Subcommand("browse") with BackupFolderOption {
    val hidden = opt[String](hidden = true)
  }
  val help = new Subcommand("help") {
    val command = trailArg[String](required = false)
  }
}

// Domain classes
case class Size(bytes: Long) {
  def this(s: String) = this(SizeParser.parse(s).bytes)
  override def toString = Utils.readableFileSize(bytes)
}

object SizeParser {
  val knownTypes: Set[Class[_]] = Set(classOf[Size])
  val patt = Pattern.compile("([\\d.]+)[\\s]*([GMK]?B)", Pattern.CASE_INSENSITIVE);

  def parse(size: String): Size = {
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
  implicit def string2Size(s: String) = SizeParser.parse(s)

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

}

trait Utils extends Logging {
  lazy val l = logger
  import Utils._

  def readableFileSize(size: Long): String = Utils.readableFileSize(size)

  def printDeleted(message: String) {
    ConsoleManager.writeDeleteLine(message)
  }

}
