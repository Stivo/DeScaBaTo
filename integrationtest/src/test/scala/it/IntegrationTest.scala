package ch.descabato.it

import java.io.File
import java.io.FileInputStream
import java.io.FileOutputStream
import java.io.RandomAccessFile
import java.nio.file.Paths
import java.util.{List => JList}

import ch.descabato._
import ch.descabato.frontend.CLI
import ch.descabato.utils.Streams
import ch.descabato.utils.Streams.DelegatingOutputStream
import ch.descabato.utils.Streams.HashingOutputStream
import ch.descabato.utils.Utils
import org.apache.commons.exec.CommandLine
import org.apache.commons.exec.DefaultExecutor
import org.apache.commons.exec.ExecuteException
import org.apache.commons.exec.ExecuteResultHandler
import org.apache.commons.exec.ExecuteWatchdog
import org.apache.commons.exec.PumpStreamHandler
import org.apache.commons.exec.environment.EnvironmentUtils
import org.scalatest.Matchers._
import org.scalatest._
import org.scalatest.prop.GeneratorDrivenPropertyChecks

import scala.collection.mutable.Set
import scala.util.Random

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

//  var baseFolder = new File("H:/testdata")
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
    val friendlyTestName = currentTestName.replace("/", "_")
    val cmdLine = new CommandLine("java")
    val libFolder = new File(packFolder, "lib")
    cmdLine.addArgument(s"-cp")
    cmdLine.addArgument(s"$libFolder/*")
    cmdLine.addArgument("ch.descabato.frontend.CLI")
    cmdLine.addArgument(args.head)
    cmdLine.addArgument("--logfile")
    cmdLine.addArgument(friendlyTestName + ".log")
    cmdLine.addArgument("--no-ansi")
    cmdLine.addArgument("--no-gui")
    args.tail.foreach { arg =>
      cmdLine.addArgument(arg)
    }
    new BackupExecutionHandler(cmdLine, packFolder, friendlyTestName, maxSeconds)
  }

  def createHandlerScript(args: Seq[String], redirect: Boolean = true, maxSeconds: Int = 600) = {
    val friendlyTestName = currentTestName.replace("/", "_")
    val cmdLine = new CommandLine(batchfile)
    cmdLine.addArgument(args.head)
    cmdLine.addArgument("--logfile")
    cmdLine.addArgument(friendlyTestName + ".log")
    cmdLine.addArgument("--no-ansi")
    cmdLine.addArgument("--no-gui")
    args.tail.foreach { arg =>
      cmdLine.addArgument(arg)
    }
    new BackupExecutionHandler(cmdLine, packFolder, friendlyTestName, maxSeconds)
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

//  "plain backup" should "work" in {
//    testWith(" --threads 1", "", 2, "100Mb")
//  }

//  "encrypted backup" should "work" in {
//    testWith(" --threads 5 --compression none --volume-size 20Mb --passphrase mypassword", " --passphrase mypassword", 2, "20Mb")
//  }
//
//  "low volumesize backup with prefix" should "work" in {
//    testWith(" --threads 5 --prefix testprefix --volume-size 1Mb --block-size 2Kb", " --prefix testprefix", 1, "20Mb")
//  }
//
////    "backup with redundancy" should "recover" in {
////      testWith(" --volume-size 10mb", "", 1, "20Mb", false, true)
////    }

  testWith("backup with smart compression", " --compression smart --threads 8 --volume-size 20Mb", "", 1, "200Mb", crash = false)

  testWith("backup with crashes", " --compression deflate --volume-size 10Mb", "", 2, "100Mb", crash = true, redundancy = false)

  testWith("backup with crashes, encryption and multiple threads",
    " --threads 10 --compression deflate --passphrase testpass --volume-size 50Mb", " --passphrase testpass", 3, "300mb", crash = true, redundancy = false)

  def numberOfCheckpoints(): Int = {
    if (backup1.exists) {
      backup1.listFiles().filter(_.getName().contains("volume_")).size
    } else {
      0
    }
  }

  def testWith(testName: String, config: String, configRestore: String, iterations: Int, maxSize: String, crash: Boolean = false, redundancy: Boolean = false) {
    val hasPassword = configRestore.contains("--passphrase")
    val fg = new FileGen(input, maxSize)
    testName should "setup" in {
      deleteAll(input)
      deleteAll(backup1)
      deleteAll(restore1)
      l.info(s"Testing with $config and $configRestore, $iterations iterations with $maxSize, crashes: $crash, redundancy: $redundancy")
      baseFolder.mkdirs()
      assume(baseFolder.getCanonicalFile().exists())
      // create some files
      fg.rescan()
      fg.generateFiles
      fg.rescan()
    }
    val maxCrashes = 3
    for (i <- 1 to iterations) {
      if (crash) {
        it should s"backup while crashing $i/$iterations" in {
          var crashes = 0
          while (crashes < maxCrashes) {
            val checkpoints = numberOfCheckpoints()
            // crash process sometimes
            val proc = createHandler(s"backup$config $backup1 $input".split(" "))
            proc.start()
            if (Random.nextBoolean) {
              val secs = Random.nextInt(10) + 5
              l.info(s"Waiting for $secs seconds before destroying process")
              var waited = 0
              while (waited < secs && !proc.finished) {
                Thread.sleep(1000)
                waited += 1
              }
            } else {
              l.info("Waiting for 2 new volume before destroying process")
              Thread.sleep(1000)
              while ((numberOfCheckpoints() <= checkpoints + 1) && !proc.finished) {
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
      }
      it should s"backup $i/$iterations" in {
        // let backup finish
        startAndWait(s"backup$config $backup1 $input".split(" ")) should be(0)
        // no temp files in backup
        input.listFiles().filter(_.getName().startsWith("temp")).toList should be('empty)
      }
      it should s"verify $i/$iterations" in {
        // verify backup
        startAndWait(s"verify$configRestore --percent-of-files-to-check 50 $backup1".split(" ")) should be(0)
      }

      if (hasPassword) {
        it should s"not verify correctly with a wrong password $i/$iterations" in {
          startAndWait(s"verify${configRestore}a --percent-of-files-to-check 50 $backup1".split(" ")) should not be (0)
        }
      }

      if (redundancy) {
        // Testing what happens when messing with the files
        messupBackupFiles()
      }

      it should s"restore correctly $i/$iterations" in {
        // restore backup to folder, folder already contains old restored files.
        startAndWait(s"restore$configRestore --restore-to-folder $restore1 $backup1".split(" ")) should be(0)
        // compare files
        compareBackups(input, restore1)
      }
      // delete some files
      it should s"setup next iteration $i/$iterations" in {
        if (i != iterations) {
          l.info("Changing files")
          fg.changeSome
          l.info("Changing files done")
        }
      }
    }
    if (!redundancy) {
      it should s"fail to verify if files are messed up" in {
        messupBackupFiles
        l.info("Verification should fail after files have been messed up")
        startAndWait(s"verify$configRestore --percent-of-files-to-check 100 $backup1".split(" "), false) should not be (0)
      }
    }
  }

  def messupBackupFiles() {
    val files = backup1.listFiles()
    val set = Set("hashlists", "files")
    val prefix = if (currentTestName contains "prefix") "testprefix_" else ""
    files.filter(x => set.exists(s => x.getName.toLowerCase().startsWith(prefix + s))).foreach { f =>
      l.info("Messing up " + f + " length " + f.length())
      val raf = new RandomAccessFile(f, "rw")
      raf.seek(raf.length() / 2)
      raf.write(("\0").getBytes)
      raf.close()
    }
    files.filter(_.getName.startsWith(prefix + "volume")).filter(_.length > 100 * 1024).foreach { f =>
      l.info("Messing up " + f)
      val raf = new RandomAccessFile(f, "rw")
      raf.seek(raf.length() / 2)
      raf.write(("\0" * 100).getBytes)
      raf.close()
    }
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
    Streams.copy(new FileInputStream(f), hos)
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
