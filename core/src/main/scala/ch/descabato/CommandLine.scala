package ch.descabato

import org.rogach.scallop._
import scala.reflect.runtime.universe.TypeTag
import java.util.regex.Pattern
import java.math.{ BigDecimal => JBigDecimal }
import javax.xml.bind.DatatypeConverter
import java.text.DecimalFormat
import com.typesafe.scalalogging.slf4j.Logging
import CLI._
import java.io.PrintStream
import scala.collection.mutable.Buffer
import java.io.File
import ScallopConverters._
import java.io.BufferedReader
import java.io.InputStreamReader
import java.io.FileOutputStream

object CLI {

  def runsInJar = classOf[CreateBackupOptions].getResource("CreateBackupOptions.class").toString.startsWith("jar:")

  def callWithReflection(args: Seq[String], className: String) = {
    val constructor = Class.forName(className).getConstructor(classOf[Seq[String]])
    constructor.newInstance(args).asInstanceOf[Command]
  }
  
  def getCommand(args: Seq[String]): Command = args.toList match {
    case "backup" :: args => new BackupCommand(args)
    case "restore" :: args => new RestoreCommand(args)
    case "browse" :: args => callWithReflection(args, "ch.descabato.browser.BrowseCommand")
    case "help" :: args => new HelpCommand(args)
    //    case "newbackup" :: args => new NewBackupCommand(args)
    case _ => new HelpCommand(Nil)
  }

  def parseCommandLine(args: Seq[String]) {
    getCommand(args).execute()
  }

  def main(args: Array[String]) {
    if (runsInJar) {
      java.lang.System.setOut(new PrintStream(System.out, true, "UTF-8"))
      parseCommandLine(args)
    } else {
      //parseCommandLine("backup --serializer-type json --compression none backups testdata".split(" "))
      //      parseCommandLine("browse -p asdf backups".split(" "))
//      parseCommandLine("restore --restore-to-folder restore --relative-to-folder . backups".split(" "))
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

  def execute()
  
  def askUser(question: String = "Do you want to continue?"): String = {
    println(question)
    val bufferRead = new BufferedReader(new InputStreamReader(System.in));
    bufferRead.readLine();
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

trait BackupRelatedCommand extends Command {
  type T <: BackupFolderOption
  
  val args: Seq[String]
  def newT: T

  //  def name : String
  final override def execute() {
    start(newT)
  }
  
  def start(t: T) {
    t.afterInit
    val conf = new BackupConfigurationHandler(t).configure
    start(t, conf)
  }

  def start(t: T, conf: BackupFolderConfiguration)

}

// Parsing classes

trait ChangeableBackupOptions extends BackupFolderOption {
  val keylength = opt[Int](default = Some(128))
  val volumeSize = opt[Size](default = Some(Size("100Mb")))
}

trait CreateBackupOptions extends ChangeableBackupOptions {
  val serializerType = opt[String]()
  val compression = opt[CompressionMode]()
  val blockSize = opt[Size](default = Some(Size("100Kb")))
  val hashAlgorithm = opt[String](default = Some("md5"))
}

trait BackupFolderOption extends ScallopConf {
  val passphrase = opt[String](default = None)
  val prefix = opt[String](default = Some(""))
  val backupDestination = trailArg[String](required = true).map(new File(_).getAbsoluteFile())
}

class SubCommandBase(name: String) extends Subcommand(name) with BackupFolderOption

class BackupCommand(val args: Seq[String]) extends BackupRelatedCommand {

  def writeBat(t: T, conf: BackupFolderConfiguration) = {
    val path = new File("descabato.bat").getAbsolutePath()
    val suffix = if (Utils.isWindows) ".bat" else ""
    val line = s"$path backup ${t.backupDestination()} ${t.folderToBackup()}"
    def writeTo(f: File) {
      val bat = new File(f, s"${conf.folder.getName()}$suffix")
      if (!bat.exists) {
        val ps = new PrintStream(new FileOutputStream(bat))
        ps.print(line)
        ps.close()
        println("A file " + bat + " has been written to execute this backup again")
      }
    }
    writeTo(new File("."))
    writeTo(conf.folder)
  }

  type T = BackupConf
  val newT = new BackupConf(args)
  def start(t: T, conf: BackupFolderConfiguration) {
    println(t.summary)
    val bdh = new BackupHandler(conf) with ZipBlockStrategy
    writeBat(t, conf)
    bdh.backup(t.folderToBackup() :: Nil)
  }
}

class RestoreCommand(val args: Seq[String]) extends BackupRelatedCommand {
  type T = RestoreConf
  val newT = new RestoreConf(args)
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

class HelpCommand(val args: Seq[String]) extends Command {
  val T = ScallopConf
  def newT = throw new IllegalAccessException("Should not be called")
    override def execute() {
      args match {
        case Nil => println(
"""The available commands are: backup, restore, browse. 
For further help about a specific command type 'help backup' or 'backup --help'.
For general usage guide go to https://github.com/Stivo/DeScaBaTo""")
        case command :: _ => CLI.parseCommandLine(command::"--help"::Nil)
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
      t.printStackTrace(ps)
      l.debug(new String(baos.toByteArray))
    })
  }

}
