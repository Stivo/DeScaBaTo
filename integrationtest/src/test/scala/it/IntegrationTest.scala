package ch.descabato.it

import org.scalatest._
import java.io.File
import org.scalatest.Matchers._
import java.util.Arrays
import scala.collection.mutable.HashMap
import scala.collection.mutable.Buffer
import scala.collection.mutable.Set
import java.io.ByteArrayOutputStream
import scala.collection.mutable.ArrayBuffer
import java.util.{ List => JList }
import java.util.ArrayList
import scala.collection.convert.DecorateAsScala
import scala.collection.JavaConversions._
import java.io.ByteArrayInputStream
import java.io.InputStream
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalacheck.Gen
import scala.util.Random
import org.scalacheck.Arbitrary
import org.scalacheck._
import Arbitrary.arbitrary
import ch.descabato._
import java.security.MessageDigest
import ch.descabato.Streams.HashingOutputStream
import java.io.FileInputStream
import java.io.RandomAccessFile
import net.java.truevfs.access.TVFS
import scala.io.Source
import java.lang.ProcessBuilder.Redirect
import org.apache.commons.exec.CommandLine
import org.apache.commons.exec.DefaultExecutor
import org.apache.commons.exec.ExecuteWatchdog
import org.apache.commons.exec.ExecuteResultHandler
import org.apache.commons.exec.ExecuteException
import ch.descabato.RichFlatSpec
import org.apache.commons.exec.PumpStreamHandler
import java.io.FileOutputStream
import ch.descabato.Streams.DelegatingOutputStream

class BackupExecutionHandler(args: CommandLine, logfolder: File, name: String, val secs: Int = 600) extends ExecuteWatchdog(secs * 1000) with ExecuteResultHandler with Utils {
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

  def startAndWait() = {
    val out = executor.execute(args)
    close()
    out
  }

  def start() {
    executor.execute(args, this)
  }

  def close() {
    out.close()
    error.close()
  }

}

class IntegrationTest extends RichFlatSpec with BeforeAndAfter with BeforeAndAfterAll with GeneratorDrivenPropertyChecks with TestUtils {
  import org.scalacheck.Gen._

  CLI._overrideRunsInJar = true

  val batchfile = new File("core/target/pack/bin/descabato").getAbsoluteFile()

  var baseFolder = new File("integrationtest/testdata")
  var logFolder = new File("integrationtest/logs")

  def folder(s: String) = new File(baseFolder, s).getAbsoluteFile()
  var input = folder("input")
  val backup1 = folder("backup1")
  val restore1 = folder("restore1")

  def createHandler(args: Seq[String], redirect: Boolean = true, maxSeconds: Int = 600) = {
    val cmdLine = new CommandLine(batchfile);
    cmdLine.addArgument(args.head)
    cmdLine.addArgument("--logfile")
    cmdLine.addArgument(currentTestName + ".log")
    args.tail.foreach { arg =>
      cmdLine.addArgument(arg)
    }
    val handler = new BackupExecutionHandler(cmdLine, logFolder, currentTestName, maxSeconds)
    handler
  }

  def startAndWait(args: Seq[String], redirect: Boolean = true) = {
    try {
      createHandler(args, redirect).startAndWait
    } catch {
      case e: ExecuteException => e.getExitValue()
    }
  }

  override def beforeAll {
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
    testWith(" --no-redundancy", "", 3, "200Mb")
  }

  "encrypted backup" should "work" in {
    testWith(" --compression gzip --volume-size 20Mb --no-redundancy --passphrase mypassword", " --passphrase mypassword", 1, "50Mb")
  }

  "low volumesize backup with prefix" should "work" in {
    testWith(" --no-redundancy --prefix testprefix --volume-size 1Mb --block-size 2Kb", " --prefix testprefix", 1, "20Mb")
  }

  "backup with multiple threads" should "work" in {
    testWith(" --no-redundancy --compression bzip2 --threads 4 --volume-size 20Mb", "", 5, "50Mb", false)
  }

  "backup with redundancy" should "recover" in {
    testWith(" --volume-size 10mb", "", 1, "20Mb", false, true)
  }

  "backup with crashes" should "work" in {
    testWith(" --no-redundancy --checkpoint-every 10Mb --volume-size 10Mb", "", 5, "300Mb", true, false)
  }

  "backup with crashes and encryption" should "work" in {
    testWith(" --no-redundancy --checkpoint-every 10Mb --volume-size 10Mb", "", 2, "200Mb", true, false)
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
    deleteAll(input)
    deleteAll(backup1)
    deleteAll(restore1)
  }

  def getFile(s1: Iterable[File], file: File) = s1.find(_.getName() == file.getName()).get

  def compareBackups(f1: File, f2: File) {
    val files1 = f1.listFiles().toSet
    val files2 = f2.listFiles().toSet
    for (c1 <- files1) {
      val c2 = getFile(files2, c1)
      if (c1.isDirectory()) {
        c2 should be('directory)
        compareBackups(c1, c2)
      } else {
        assert(c1.getName === c2.getName, "name should be same for " + c1)
        assert(c1.length === c2.length, "length should be same for " + c1)
        assert(c1.lastModified === c2.lastModified +- 1000, "Last modified should be within a second")
        assert(hash(c1) === hash(c2), "content should be same for " + c1)
      }
    }
  }

  def hash(f: File) = {
    val hos = new HashingOutputStream("md5")
    val buf = Streams.copy(new FileInputStream(f), hos)
    Utils.encodeBase64(hos.out.get)
  }

}
