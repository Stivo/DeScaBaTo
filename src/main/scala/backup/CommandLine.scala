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

trait ExplainHelp extends FieldArgs {
  override def helpMessage = {
    val lines = super.helpMessage.lines
    val out = new StringBuilder()
    out ++= lines.next+"\n"
    out ++= "(Commands with -argX shortcuts should be named last)\n"
    out ++= lines.mkString("\n")
    out.toString
  }
}

case class Size(bytes: Long) {
  def this() = this(-1L)
  override def toString = Test.readableFileSize(bytes)
}

object SizeParser extends SimpleParser[Size] {
  val knownTypes: Set[Class[_]] = Set(classOf[Size])
  val patt = Pattern.compile("([\\d.]+)([GMK]B)", Pattern.CASE_INSENSITIVE);
  
  def parse(size: String) = {
	var out : Long = -1;
    val matcher = patt.matcher(size);
    val map = List(("GB", 3), ("MB", 2),("KB", 1), ("", 0)).toMap
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

trait BackupFolderOption extends FileHandlingOptions {
  @Arg(shortcut="arg1")
  @Required
  var backupFolder: File = null
  
  def getBlockStrategy() : BlockStrategy = new ZipBlockStrategy(this)
  
}

class BackupOptions extends ExplainHelp with BackupFolderOption with EncryptionOptions {
  
  @Arg(shortcut="arg2")
  @Required
  var folderToBackup: List[File] = _
  
  var volumeSize : Size = SizeParser.parse("64Mb");
  
  var blockSize : Size = new Size(1024*1024);
    
  override def toString() = {
    s"Backing up folder $folderToBackup to $backupFolder (using $compression, "+ 
    s"$blockSize blocks). "
    .+(super[EncryptionOptions].toString)
  }
  
  override def getBlockStrategy() = new ZipBlockStrategy(this, Some(volumeSize))
  
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

class FindOptions extends ExplainHelp with BackupFolderOption with EncryptionOptions {
    
  @Arg(shortcut="arg2")
  @Required
  var filePattern: String = _
  
  override def toString() = {
    s"Finding files in backup $backupFolder with pattern $filePattern (using $compression). "
    .+(super[EncryptionOptions].toString)
  }
  
}

trait CompressionOptions extends FieldArgs {
  @Arg(shortcut="c")
  var compression : CompressionMode = CompressionMode.none
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

trait FileHandlingOptions extends CompressionOptions with EncryptionOptions with HashOptions

trait Command {
  def name: String
  def execute(a: Array[String])
}

trait OptionCommand extends Command {
  type T <: FieldArgs
  def numOfArgs: Int
  def prepareArgs(a: Array[String]) = {
    var list = a.toList
    var append : List[String] = Nil
    for (i <- numOfArgs to (1, -1)) {
      val last = list.last
      list = list.dropRight(1)
      append = s"-arg$i" :: last :: append
    }
    (list ++ append).toArray
  }
  
  def getNewOptions : T
  
  def execute(a: Array[String]) {
    val args = getNewOptions
    val prepared = prepareArgs(a)
    args.parse(prepared)
    execute(args)
  }
  
  def execute(t: T)
  
  def askUser = {
    println("Do you want to continue?")
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

class BackupCommand extends OptionCommand {
  type T = BackupOptions
  def numOfArgs = 2
  def name = "Backup"
  def getNewOptions = new T()
  def execute(args: T) {
    println(args)
    if (askUser) {
      val bh = new BackupHandler(args)
      bh.backupFolder()
    } 
  }
}

class RestoreCommand extends OptionCommand {
  type T = RestoreOptions
  def numOfArgs = 2
  def name = "Restore"
  def getNewOptions = new T()
  def execute(args: T) {
    println(args)
    if (askUser) {
      val bh = new RestoreHandler(args)
      bh.restoreFolder()
    } 
  }
}

class FindCommand extends OptionCommand {
  type T = FindOptions
  def numOfArgs = 2
  def name = "Find"
  def getNewOptions = new T()
  def execute(args: T) {
    println(args)
    val bh = new SearchHandler(args)
    bh.findInBackup(args)
  }
}

object CommandLine {
  
  def verifyBlock(f: File) {
    val reader = new ZipFileReader(f)
    reader.names.foreach{x => 
      val bytes = reader.getBytes(x).get
      val md = MessageDigest.getInstance("SHA-256")
      val gzip = new ByteArrayInputStream(bytes)
      var lastRead = 1
      while (lastRead > 0) {
      val buf = Array.ofDim[Byte](1024)
      lastRead = gzip.read(buf)
      if (lastRead > 0)
    	  md.update(buf, 0, lastRead)
      }
      val out = md.digest()
      println(ByteHandling.encodeBase64Url(out)+" == "+x)
    }
  }
  
  def prepareCommands = {
    val list = Buffer[OptionCommand]()
    ParseHelper.registerParser(SizeParser)
    list += new BackupCommand()
    list += new RestoreCommand()
    list += new FindCommand()
    list.map(x => (x.name.toLowerCase(), x)).toMap
  }
  def parseCommandLine(args: Array[String]) {
    val map = prepareCommands
    val (first, tail) = args.splitAt(1)
    map(first(0)).execute(tail)
  }
  def printAllHelp = println(prepareCommands.values.map(_.getNewOptions.helpMessage).mkString("\n"))
  def printHelp(x: Array[String]) = println(prepareCommands(x(0)).getNewOptions.helpMessage)
  def runsInJar = classOf[FindCommand].getResource("FindCommand.class").toString.startsWith("jar:")
  def main(args: Array[String]) {
    
    if (runsInJar) {
      parseCommandLine(args)
    } else {
//        verifyBlock(new File("e:/temp/test/volume_23.zip"))
	    var a = Buffer[String]()
	    
	    a += "backup"
//	    a ++= "-c" :: "zip" :: Nil
	    a ++= "--hashAlgorithm" :: "md5" :: Nil 
	    a ++= "--passphrase" :: "password" :: Nil
	    a ++= "--blockSize" :: "10Kb" :: Nil
	    a ++= "--volumeSize" :: "1Mb" :: Nil
	    a ++= "backups" :: "test" :: Nil
	    parseCommandLine(a.toArray)
	    a.clear()
	    a += "restore"
//	    a ++= "-c" :: "zip" :: Nil
	    a ++= "--hashAlgorithm" :: "md5" :: Nil 
	    a ++= "--passphrase" :: "password" :: Nil
	    a ++= "--keyLength" :: "128" :: Nil
	    a ++= "--relativeToFolder" :: "" :: Nil
	    a ++= "backups" :: "restore" :: Nil
//	    printAllHelp
	    parseCommandLine(a.toArray)
    }
  }
}