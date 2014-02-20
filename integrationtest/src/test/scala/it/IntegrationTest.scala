package ch.descabato.it

import org.scalatest._
import java.io.File
import org.scalatest.Matchers._
import scala.collection.mutable.Set
import java.util.{ List => JList }
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import scala.util.Random
import ch.descabato._
import ch.descabato.utils.Streams.{DelegatingOutputStream, HashingOutputStream}
import ch.descabato.utils.Utils._
import ch.descabato.utils.{Streams, Utils}
import java.io.FileInputStream
import java.io.RandomAccessFile
import net.java.truevfs.access.TVFS
import org.apache.commons.exec.CommandLine
import org.apache.commons.exec.DefaultExecutor
import org.apache.commons.exec.ExecuteWatchdog
import org.apache.commons.exec.ExecuteResultHandler
import org.apache.commons.exec.ExecuteException
import org.apache.commons.exec.PumpStreamHandler
import java.io.FileOutputStream
import java.nio.file.Paths
import org.apache.commons.exec.environment.EnvironmentUtils
import ch.descabato.frontend.CLI

class IntegrationTest extends FlatSpec with RichFlatSpecLike with BeforeAndAfter with BeforeAndAfterAll with GeneratorDrivenPropertyChecks with TestUtils {

  CLI._overrideRunsInJar = true

  val suffix = if (Utils.isWindows) ".bat" else ""
  
  val descabatoFolder = {
    val folder = Paths.get("").toAbsolutePath().toFile()
    if (folder.listFiles().map(_.getName()).contains("core")) {
      folder
    } else {
      folder.getParentFile()
    }
  }
    
  val batchfile = new File(descabatoFolder, s"core/target/pack/bin/descabato$suffix").getAbsoluteFile()
  val packFolder = new File(descabatoFolder, s"core/target/pack").getAbsoluteFile()

  var baseFolder = new File("integrationtest/testdata")
  var logFolder = new File(descabatoFolder, "integrationtest/logs")

  def folder(s: String) = new File(baseFolder, s).getAbsoluteFile()
  var input = folder("input")
  val backup1 = folder("backup1")
  val restore1 = folder("restore1")

  def createHandler(args: Seq[String], redirect: Boolean = true, maxSeconds: Int = 600) = {
    if (Utils.isWindows)
      createHandlerJava(args, redirect, maxSeconds)
    else
      createHandlerScript(args, redirect, maxSeconds)
  }

  def createHandlerJava(args: Seq[String], redirect: Boolean = true, maxSeconds: Int = 600) = {
    val cmdLine = new CommandLine("java");
    val libFolder = new File(packFolder, "lib")
    cmdLine.addArgument(s"-cp")
    cmdLine.addArgument(s"$libFolder/*")
    cmdLine.addArgument("ch.descabato.frontend.CLI")
    cmdLine.addArgument(args.head)
    cmdLine.addArgument("--logfile")
    cmdLine.addArgument(currentTestName + ".log")
    cmdLine.addArgument("--noansi")
    cmdLine.addArgument("--no-gui")
    args.tail.foreach { arg =>
      cmdLine.addArgument(arg)
    }
    new BackupExecutionHandler(cmdLine, packFolder, currentTestName, maxSeconds)
  }

  def createHandlerScript(args: Seq[String], redirect: Boolean = true, maxSeconds: Int = 600) = {
    val cmdLine = new CommandLine(batchfile);
    cmdLine.addArgument(args.head)
    cmdLine.addArgument("--logfile")
    cmdLine.addArgument(currentTestName + ".log")
    cmdLine.addArgument("--noansi")
    cmdLine.addArgument("--no-gui")
    args.tail.foreach { arg =>
      cmdLine.addArgument(arg)
    }
    new BackupExecutionHandler(cmdLine, packFolder, currentTestName, maxSeconds)
  }

  def startAndWait(args: Seq[String], redirect: Boolean = true) = {
    try {
      createHandler(args, redirect).startAndWait
    } catch {
      case e: ExecuteException => e.getExitValue()
    }
  }

  override def beforeAll {
    val destfile = new File(descabatoFolder, "core/target/scala-2.10/jacoco/jacoco.exec")
    destfile.delete()
    System.setProperty("logname", "integrationtest.log")
    deleteAll(logFolder)
    logFolder.mkdirs()
  }

  before {
    deleteAll(input)
    deleteAll(backup1)
    deleteAll(restore1)
  }

  "plain backup" should "work" in {
    testWith(" --threads 1", "", 5, "100Mb")
  }

  "encrypted backup" should "work" in {
    testWith(" --threads 5 --compression gzip --volume-size 20Mb --passphrase mypassword", " --passphrase mypassword", 5, "50Mb")
  }

  "low volumesize backup with prefix" should "work" in {
    testWith(" --threads 5 --prefix testprefix --volume-size 1Mb --block-size 2Kb", " --prefix testprefix", 1, "20Mb")
  }

  "backup with multiple threads" should "work" in {
    testWith(" --compression bzip2 --threads 8 --volume-size 20Mb", "", 5, "50Mb", false)
  }

//    "backup with redundancy" should "recover" in {
//      testWith(" --volume-size 10mb", "", 1, "20Mb", false, true)
//    }

  "backup with crashes" should "work" in {
    testWith(" --volume-size 10Mb", "", 5, "300Mb", true, false)
  }

  "backup with crashes, encryption and multiple threads" should "work" in {
    testWith(" --threads 10 --volume-size 10Mb", "", 2, "200Mb", true, false)
  }

  def numberOfCheckpoints(): Int = {
    if (backup1.exists) {
      backup1.listFiles().filter(_.getName().contains("temp.hash")).size
    } else {
      0
    }
  }

  def testWith(config: String, configRestore: String, iterations: Int, maxSize: String, crash: Boolean = false, redundancy: Boolean = false) {
    l.info("")
    l.info("")
    l.info("")
    l.info(s"Testing with $config and $configRestore, $iterations iterations with $maxSize, crashes: $crash, redundancy: $redundancy")
    baseFolder.mkdirs()
    assume(baseFolder.getCanonicalFile().exists())
    deleteAll(backup1)
    deleteAll(restore1)
    // create some files
    val fg = new FileGen(input, maxSize)
    fg.generateFiles
    fg.rescan()
    val maxCrashes = 10
    for (i <- 1 to iterations) {
      l.info(s"Iteration $i of $iterations")
      if (crash) {
        var crashes = 0
        while (crashes < maxCrashes) {
          val checkpoints = numberOfCheckpoints()
          // crash process sometimes
          val proc = createHandler(s"backup$config $backup1 $input".split(" "))
          proc.start()
          if (Random.nextBoolean) {
            val secs = Random.nextInt(10) + 2
            l.info(s"Waiting for $secs seconds before destroying process")
            var waited = 0
            while (waited < secs && !proc.finished) {
              Thread.sleep(1000)
              waited += 1
            }
          } else {
            l.info("Waiting for new hashlist before destroying process")
            Thread.sleep(1000)
            while ((numberOfCheckpoints() == checkpoints) && !proc.finished) {
              Thread.sleep(100)
            }
          }
          crashes += 1
          if (proc.finished) {
            crashes = 10
          }
          proc.destroyProcess()
        }
        l.info("Crashes done, letting process finish now")
      }
      // let backup finish
      startAndWait(s"backup$config $backup1 $input".split(" ")) should be(0)
      // no temp files in backup
      input.listFiles().filter(_.getName().startsWith("temp")).toList should be('empty)
      // verify backup
      startAndWait(s"verify$configRestore --percent-of-files-to-check 50 $backup1".split(" ")) should be(0)

      if (redundancy) {
        // Testing what happens when messing with the files
        messupBackupFiles()
      }
      // restore backup to folder, folder already contains old restored files.
      startAndWait(s"restore$configRestore --restore-to-folder $restore1 $backup1".split(" ")) should be(0)
      // compare files
      compareBackups(input, restore1)
      // delete some files
      if (i != iterations) {
        l.info("Changing files")
        fg.changeSome
        l.info("Changing files done")
      }
    }
    if (!redundancy) {
      messupBackupFiles
      l.info("Verification should fail after files have been messed up")
      startAndWait(s"verify$configRestore --percent-of-files-to-check 50 $backup1".split(" "), false) should not be (0)
      l.info("Verification failed as expected")
    }
  }

  def messupBackupFiles() {
    val files = backup1.listFiles()
    val set = Set("hashlists", "files")
    val prefix = if (currentTestName contains "prefix") "testprefix_" else ""
    files.filter(x => set.exists(s => x.getName.toLowerCase().startsWith(prefix + s))).foreach { f =>
      l.info("Messing up " + f + " length " + f.length())
      val raf = new RandomAccessFile(f, "rw")
      raf.seek(raf.length() / 2);
      raf.write(("\0").getBytes)
      raf.close()
    }
    files.filter(_.getName.startsWith(prefix + "volume")).filter(_.length > 100 * 1024).foreach { f =>
      l.info("Messing up " + f)
      val raf = new RandomAccessFile(f, "rw")
      raf.seek(raf.length() / 2);
      raf.write(("\0" * 100).getBytes)
      raf.close()
    }
  }

  after {
    try {
      TVFS.umount()
    } catch {
      case e: Exception =>
        l.warn("Exception while unmounting truevfs ")
        logException(e)
    }
//    deleteAll(input)
//    deleteAll(backup1)
//    deleteAll(restore1)
  }

  def getFile(s1: Iterable[File], file: File) = s1.find(_.getName() == file.getName()).get

  def compareBackups(f1: File, f2: File) {
    val files1 = f1.listFiles().toSet
    val files2 = f2.listFiles().toSet
    for (c1 <- files1) {
      val c2 = getFile(files2, c1)
      if (c1.isDirectory()) {
        c2 should be ('directory)
        compareBackups(c1, c2)
      } else {
        assert(c1.getName === c2.getName, "name should be same for " + c1)
        assert(c1.length === c2.length, "length should be same for " + c1)
        assert(c1.lastModified === c2.lastModified +- 1000, "Last modified should be within a second for "+c1)
        assert(hash(c1) === hash(c2), "content should be same for " + c1)
      }
    }
  }

  def hash(f: File) = {
    val hos = new HashingOutputStream("md5")
    val buf = Streams.copy(new FileInputStream(f), hos)
    Utils.encodeBase64Url(hos.out.get)
  }

}

class BackupExecutionHandler(args: CommandLine, logfolder: File, name: String, val secs: Int = 600)
  extends ExecuteWatchdog(secs * 1000) with ExecuteResultHandler with Utils {
  private val executor = new DefaultExecutor()
  executor.setWatchdog(this)
  executor.setWorkingDirectory(logfolder)
  val out = new FileOutputStream(new File(logfolder, name + "_out.log"), true)
  val error = new FileOutputStream(new File(logfolder, name + "_error.log"), true)
  val streamHandler = new PumpStreamHandler(new DelegatingOutputStream(out, System.out),
    new DelegatingOutputStream(error, System.err))
  executor.setStreamHandler(streamHandler)
  @volatile var finished = false
  var exit = Int.MinValue

  def onProcessComplete(exitValue: Int) {
    finished = true
    exit = exitValue
    close()
  }

  override def destroyProcess() {
    super.destroyProcess()
    close()
  }

  def onProcessFailed(e: ExecuteException) {
    logException(e)
    finished = true
    exit = -1
    close()
  }

  val baseFolder = logfolder.getParentFile().getParentFile()
  //val jacoco = new File(baseFolder, "jacocoagent.jar")
  //val destfile = new File(baseFolder, "core/target/scala-2.10/jacoco/jacoco.exec")
  //jacoco.exists() should be
  val map = EnvironmentUtils.getProcEnvironment()
  //EnvironmentUtils.addVariableToEnvironment(map, s"JVM_OPT=-javaagent:$jacoco=destfile=$destfile,append=true")

  def startAndWait() = {
    val out = executor.execute(args, map)
    close()
    out
  }

  def start() {
    executor.execute(args, map, this)
  }

  def close() {
    out.close()
    error.close()
  }

}
