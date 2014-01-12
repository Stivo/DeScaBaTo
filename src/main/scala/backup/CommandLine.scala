package backup;

import com.quantifind.sumac.FieldArgs
import com.quantifind.sumac.Arg
import scala.collection.mutable.Buffer
import java.io.File
import com.quantifind.sumac.FileParser
import com.quantifind.sumac.validation.Required
import com.quantifind.sumac.Parser
import java.util.regex.Pattern
import com.quantifind.sumac.SimpleParser
import java.math.{BigDecimal => JBigDecimal}
import java.io.BufferedReader
import java.io.InputStreamReader
import java.security.MessageDigest
import com.quantifind.sumac.ParseHelper
import java.io.FileInputStream
import java.io.ByteArrayInputStream
import java.util.zip.GZIPInputStream
import com.quantifind.sumac.Args
import java.util.Properties
import java.io.{FileOutputStream, PrintStream}
import com.quantifind.sumac.ExternalConfig
import java.io.BufferedInputStream
import com.quantifind.sumac.ExternalConfigUtil
import akka.actor.ActorSystem
import akka.actor.Props
import Utils._

trait PropertiesConfig extends ExternalConfig {
  self: Args =>
    
  import collection.JavaConverters._
  import collection.Map
  
  var propertyFile: File = _

  var propertyFileOverrides = false
  
  abstract override def readArgs(originalArgs: Map[String,String]): Map[String,String] = {
    parse(originalArgs, false)

    val props = new Properties()
    if (propertyFile != null) {
      val in = new BufferedInputStream(new FileInputStream(propertyFile))
      props.load(in)
      in.close()
    }
    //append args we read from the property file to the args from the command line, and pass to next trait
    val applyMap = if (propertyFileOverrides) {
      val map = props.asScala
      val set = self.getArgs("").map(_.getName).toSet
      val rest = map.filterKeys(set contains)
      parse(rest, false)
      rest
    } else {
      ExternalConfigUtil.mapWithDefaults(originalArgs, props.asScala)
    }
    super.readArgs(applyMap)
  }
}

trait ExplainHelp extends FieldArgs {
  
  def name = this.getClass().getSimpleName().dropRight("Options".length).toLowerCase()
  
  override def helpMessage = {
    val lines = super.helpMessage.lines.toList
    // Sorting the -arg arguments first in the list
    val regex = "\\(-arg(\\d+)"r
    val withArgs = lines.drop(1).filter(_.contains("(-arg"))
    val sorted = withArgs.toArray.sortBy{x => regex.findFirstMatchIn(x).get.group(1).toInt}
    val sortedOptions = List("usage "+name+":")++List("(Commands with -argX shortcuts can be named last unnamed)")++
    		sorted++lines.drop(1).filter(x => !sorted.contains(x))
    sortedOptions.mkString("\n")
  }
}

case class Size(bytes: Long) {
  def this() = this(-1L)
  override def toString = Utils.readableFileSize(bytes)
}

object SizeParser extends SimpleParser[Size] {
  val knownTypes: Set[Class[_]] = Set(classOf[Size])
  val patt = Pattern.compile("([\\d.]+)[\\s]*([GMK]?B)", Pattern.CASE_INSENSITIVE);

  def parse(size: String) = {
	var out : Long = -1;
    val matcher = patt.matcher(size);
    val map = List(("GB", 3), ("MB", 2),("KB", 1), ("B", 0)).toMap
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

trait BackupFolderOption extends FileHandlingOptions with PropertiesConfig {
  import akka.pattern.{ ask, pipe }
  import scala.concurrent.duration._
  
  // For test purposes
  var _fileManager : FileManager = null
  
  def fileManager = {
    if(_fileManager == null) 
    	_fileManager = new FileManager(this)
    _fileManager
  }
  
  var useDeltas = false
  
  val remote = new RemoteOptions()
  
  def configureRemoteHandler = {
    ask(Actors.remoteManager, Configure(this))(5 seconds)
  }
  
  var noprompt = false
  
  @Arg(shortcut="arg1")
  @Required
  var backupFolder: File = null
  
  def getBlockStrategy() : BlockStrategy = new ZipBlockStrategy(this)
  
  var savePropertiesFile: File = null
  
  def saveConfigFile(file: File = savePropertiesFile) {
    if (file != null)
    	PropertiesConfigCopy.saveConfig(this, file)
  }
  
  @Arg(description="try json or smile")
  var serializerType = "smile"
  
  def serialization = {
    serializerType.toLowerCase() match {
      case "json" => new JsonSerialization()
      case "smile" => new SmileSerialization()
      case x => throw new IllegalArgumentException("Serializer "+x+" does not exist")
    }
    
  }

}

object PropertiesConfigCopy {
  def saveConfig(args: FileHandlingOptions, propertyFile: File) {
    val props = new Properties()
    // passphrase should not be saved
    args.getStringValues
    	.filter(_._1 != "passphrase")
    	.filter(_._1 != "savePropertiesFile")
    	.foreach{case(k,v) => props.put(k,v)}
    val out = new FileOutputStream(propertyFile)
    props.store(out, "")
    out.close()
  }
}

class BackupOptions extends ExplainHelp with BackupFolderOption with EncryptionOptions {
  
  val redundancy = new RedundancyOptions()
  
  @Arg(shortcut="arg2")
  @Required
  var folderToBackup: List[File] = _
  
  var volumeSize : Size = "64Mb";
  
  var blockSize : Size = "1MB";
  
  var saveIndexEveryNFiles = 1000
  
  @Arg(description="If set to true, no actual backup will be performed, just an index of the files will be created")
  var onlyIndex : Boolean = false
  
  override def toString() = {
    s"Backing up folder $folderToBackup to $backupFolder (using $compression, "+ 
    s"$blockSize blocks). "
    .+(super[EncryptionOptions].toString)
  }
  
  var saveMetadata = false
  
  override def getBlockStrategy() = new ZipBlockStrategy(this, Some(volumeSize))
  
}

class RemoteOptions extends FieldArgs {
  def enabled = url.isDefined
  var url : Option[String] = None
  var localMaximumSize : Size = "1GB"
}

class RestoreOptions extends ExplainHelp with BackupFolderOption with EncryptionOptions {
    
  @Arg(shortcut="arg2")
  @Required
  var restoreToFolder: File = _
  
  var relativeToFolder: Option[File] = None
  
  override def toString() = {
    val relativeTo = if (relativeToFolder.isDefined) s", relative to $relativeToFolder" else ""
    s"Restoring backup from $backupFolder to ${restoreToFolder}${relativeTo} (using $compression compression). "
    .+(super[EncryptionOptions].toString)
  }
}

class RedundancyOptions extends FieldArgs {
	var percentage : Int = 5
	var blockSize : Size = "1MB"
	var par2Executable = new File("tools/par2.exe")
	var volumesToParTogether = 21
	var enabled = false
}

class FindOptions extends ExplainHelp with BackupFolderOption with EncryptionOptions {
    
  @Arg(shortcut="arg2")
  @Required
  var filePattern: String = _
  
  override def toString() = {
    s"Finding files in backup $backupFolder with pattern $filePattern (using $compression). "
    .+(super[EncryptionOptions].toString)
  }
	  
}

class FindDuplicateOptions extends ExplainHelp with BackupFolderOption with EncryptionOptions {
  
  var filePatternToDelete: String = null
  var filePatternToKeep: String = null
  
  var minSize = Size(20*1024)
  
  var action: DuplicateAction = null
  
  var dryrun: Boolean = true
  
  override def toString() = {
    val desc = if (action == DuplicateAction.report) {
      "report all identical files"
    } else {
      var operation = action match {
        case DuplicateAction.delete => "delete"
        case DuplicateAction.moveToTrash => "move to trash"
      }
      if (filePatternToDelete == null) {
        throw new IllegalArgumentException("File pattern to delete has to be defined")
      }
      operation + s" all files that match $filePatternToDelete, but keep all files that match $filePatternToKeep (keep is stronger)."
    }
    s"Finding duplicates in index $backupFolder with minimum size $minSize. When found, $desc"
  }
  
}

trait CompressionOptions extends FieldArgs {
  @Arg(shortcut="c")
  var compression : CompressionMode = CompressionMode.zip
}

trait EncryptionOptions extends FieldArgs {
  var passphrase: String = null
  var algorithm: String = "AES"
  var keyLength: Int = 128
  override def toString() = if (passphrase == null)
    "No Encryption"
  else
    s"""Encryption with passphrase "$passphrase" and algorithm $algorithm ($keyLength bits)"""
} 

trait HashOptions extends FieldArgs {
  var hashAlgorithm : HashMethod = HashMethod.md5
  def getHashAlgorithm = hashAlgorithm.toString().toUpperCase().replace("SHA", "SHA-")
  def getMessageDigest = MessageDigest.getInstance(getHashAlgorithm)
  def hashLength = getMessageDigest.getDigestLength()
}

class HelpOptions extends FieldArgs {
  @Arg(shortcut="arg1")
  var command : String = _
}

trait FileHandlingOptions extends CompressionOptions with EncryptionOptions with HashOptions

trait Command {
  def name: String
  def execute(a: Array[String])
}

trait OptionCommand extends Command {
  type T <: FieldArgs
  
  def name = this.getClass().getSimpleName().dropRight("Command".length)
  
  def numOfArgs: Int
  def argsAreOptional: Boolean = true
  
  def prepareArgs(a: Array[String]) = {
    var list = a.toList
    var append : List[String] = Nil
    if (a.length == 0 && argsAreOptional) {
    	// If args are optional, they may be omitted.
    	// needed for help, which can be called with the name of a command or without
    } else if (a.length < numOfArgs) {
      System.err.println(getNewOptions.helpMessage)
      throw new IllegalArgumentException()
    } else {
       for (i <- numOfArgs to (1, -1)) {
	     val last = list.last
	     if (last.startsWith("-")) {
	    	 System.err.println(getNewOptions.helpMessage)
	    	 throw new IllegalArgumentException("Main arguments are not in last position")
	     }
	     list = list.dropRight(1)
	     append = s"-arg$i" :: last :: append
	   }
    }
    (list ++ append).toArray
  }
  
  def getNewOptions : T
  
  def execute(a: Array[String]) {
    val args = getNewOptions
    val prepared = prepareArgs(a)
    try {
      args.parse(prepared)
      execute(args)
    } catch {
      case e: Exception =>
        e.printStackTrace()
        System.err.println(e.getMessage())
        System.err.println(args.helpMessage)
    }
  }
  
  def execute(t: T)
  
}

trait BackupOptionCommand extends OptionCommand {
  type T <: BackupFolderOption
  
  final def execute(t: T) {
	// if savePropertiesFile is set, it will be saved here
    executeCommand(t)
    t.saveConfigFile()
  }
  
  def executeCommand(t: T)
  
  def askUser(t: T, question: String = "Do you want to continue?") : Boolean = {
    if (t.noprompt) {
      return true
    }
    println(question)
    val bufferRead = new BufferedReader(new InputStreamReader(System.in));
	val s = bufferRead.readLine();
	val yes = Set("yes", "y")
	if (yes.contains(s.toLowerCase().trim)) {
	  true
	} else {
	  println("User aborted") 
	  false
	}
  }

    
}

class BackupCommand extends BackupOptionCommand {
  type T = BackupOptions
  def numOfArgs = 2
  def getNewOptions = new T()
  def executeCommand(args: T) {
    println(args)
    if (askUser(args)) {
      args.folderToBackup = args.folderToBackup.map(_.getAbsoluteFile())
      val bh = new BackupHandler(args)
      bh.backupFolder()
    } 
  }
}

class RestoreCommand extends BackupOptionCommand {
  type T = RestoreOptions
  def numOfArgs = 2
  def getNewOptions = new T()
  def executeCommand(args: T) {
    println(args)
    if (askUser(args)) {
      val bh = new RestoreHandler(args)
      bh.restoreFolder()
    } 
  }
}

class FindCommand extends BackupOptionCommand {
  type T = FindOptions
  def numOfArgs = 2
  def getNewOptions = new T()
  
  def executeCommand(args: T) {
    println(args)
    val bh = new SearchHandler(args, null)
    val bufferRead = new BufferedReader(new InputStreamReader(System.in));
    while (true) {
      args.filePattern = bufferRead.readLine()
      bh.searchForPattern(args.filePattern)
    }
  }
}

class FindDuplicatesCommand extends BackupOptionCommand {
  type T = FindDuplicateOptions
  def numOfArgs = 1
  def getNewOptions = new T()
  
  def executeCommand(args: T) {
    println(args)
    if (!args.dryrun && args.action != DuplicateAction.report) {
    	askUser(args, "This will delete or move files to trash, are you sure you want to continue?")
    }
    val bh = new SearchHandler(null, args)
    bh.findDuplicates
  }
}

class HelpCommand(list: Buffer[OptionCommand]) extends OptionCommand {
  type T = HelpOptions
  def numOfArgs = 1
  override val argsAreOptional = true
  
  def getNewOptions = new T()
  def execute(args: T) {
    var listCopy = list
    if (args.command != null) {
      listCopy = list.filter(_.name.toLowerCase() == args.command.toLowerCase())
      if (listCopy.isEmpty) {
        noCommandFound(args.command)
      } else {
        System.err.println(listCopy.head.getNewOptions.helpMessage)
      }
    } else {
      noCommandFound()
    }
  }
  def noCommandFound(c: String = null) {
    if (c != null) {
      System.err.println(s"$c is not a valid command.")
    }
    System.err.println(s"Valid commands are:")
    list.map(_.name.toLowerCase()).foreach(System.err.println)
  }
}


object CommandLine {
  
  def prepareCommands = {
    val list = Buffer[OptionCommand]()
    ParseHelper.registerParser(SizeParser)
    list += new BackupCommand()
    list += new RestoreCommand()
    list += new FindCommand()
    list += new FindDuplicatesCommand()
    list += new HelpCommand(list)
    list.map(x => (x.name.toLowerCase(), x)).toMap
  }
  def parseCommandLine(args: Array[String]) {
    val map = prepareCommands
    if (args.isEmpty || !map.contains(args(0))) {
      val help = map("help").asInstanceOf[HelpCommand]
      help.execute(if (args.isEmpty) Array[String]() else args.tail)
    } else {
      map(args(0)).execute(args.drop(1))
    }
  }
  def printAllHelp = println(prepareCommands.mkString("\n"))
  
  def printHelp(x: Array[String]) = println(prepareCommands(x(0)).getNewOptions.helpMessage)
  def runsInJar = classOf[FindCommand].getResource("FindCommand.class").toString.startsWith("jar:")
  
  def main(args: Array[String]) {
    
    if (runsInJar) {
      java.lang.System.setOut(new PrintStream(System.out, true, "UTF-8"))
      ConsoleManager.initAnsi
      println(
"""    DeScaBaTo  Copyright (C) 2014  Stefan Ackermann
    This program comes with ABSOLUTELY NO WARRANTY.
    This is free software, and you are welcome to redistribute it
    conforming to GPL v3.""")
      parseCommandLine(args)
    } else {
	    var a = Buffer[String]()
	    
//	    a += "help"
	    a += "backup"
//	    a ++= "--propertyFile" :: "find.properties" :: Nil
//	    a ++= "--onlyIndex" :: "true" :: Nil
	    a ++= "-c" :: "none" :: Nil
	    a ++= "--hashAlgorithm" :: "md5" :: Nil 
	    a ++= "--blockSize" :: "10Kb" :: Nil
	    a ++= "--volumeSize" :: "1Mb" :: Nil
//	    a ++= "--passphrase" :: "password" :: Nil
	    a ++= "backups" :: "test" :: Nil
//	    a ++= "backups" :: "." :: Nil
	    parseCommandLine(a.toArray)
	    
	    a.clear()
	    a += "restore"
//	    a ++= "-c" :: "zip" :: Nil
//	    a ++= "--hashAlgorithm" :: "md5" :: Nil 
//	    a ++= "--passphrase" :: "password" :: Nil
//	    a ++= "--keyLength" :: "128" :: Nil
//	    a ++= "--relativeToFolder" :: "" :: Nil
	    a ++= "backups" :: "restore" :: Nil
//	    a ++= "backups" :: "." :: Nil
//	    printAllHelp
	    parseCommandLine(a.toArray)
    }
  }
}