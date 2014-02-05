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

class IntegrationTest extends FlatSpec with BeforeAndAfter with GeneratorDrivenPropertyChecks with TestUtils {
  import org.scalacheck.Gen._

  CLI._overrideRunsInJar = true
  CLI.testMode = true

  val batchfile = new File("core/target/pack/bin/descabato").getAbsoluteFile()
  
  var baseFolder = new File("integrationtest/testdata")


  def folder(s: String) = new File(baseFolder, s).getAbsoluteFile()
  var input = folder("input")
  val backup1 = folder("backup1")
  val restore1 = folder("restore1")
  
  def startProg(args: Seq[String]) = {
     val proc = new ProcessBuilder().command((List(batchfile.getAbsolutePath()) ++ args): _*)
     .redirectOutput(Redirect.INHERIT)
     .redirectError(Redirect.INHERIT)
      .start
      proc
  }
  
  def startAndWait(args: Seq[String]) = {
    val proc = startProg(args)
    proc.waitFor()
  }
  
  before {
    deleteAll(input)
    deleteAll(backup1)
    deleteAll(restore1)
  }
  
  "plain backup" should "work" in {
	 testWith(" --no-redundancy", "", 5, "500Mb")
  }

  "encrypted backup" should "work" in {
    testWith(" --compression gzip --volume-size 20Mb --no-redundancy --passphrase mypassword", " --passphrase mypassword", 3, "50Mb")
  }

  "low volumesize backup with prefix" should "work" in {
    testWith(" --compression bzip2 --no-redundancy --prefix testprefix --volume-size 1Mb --block-size 2Kb", " --prefix testprefix", 2, "20Mb")
  }

  "backup with multiple threads" should "work" in {
    testWith(" --no-redundancy --compression bzip2 --threads 4 --volume-size 20Mb", "", 5, "50Mb", false)
  }

  "backup with redundancy" should "recover" in {
    testWith(" --volume-size 10mb", "", 1, "20Mb", false, true)
  }
  
  "backup with crashes" should "work" in {
	testWith(" --checkpoint-every 10Mb --volume-size 10Mb", "", 5, "500Mb", true)
  }

  "backup with crashes and encryption" should "work" in {
	testWith(" --checkpoint-every 10Mb --volume-size 10Mb", "", 5, "200Mb", true)
  }

  def numberOfCheckpoints() = {
    if (backup1.exists) {
      backup1.listFiles().filter(_.getName().contains("temp.hash"))
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
    for (i <- 1 to iterations) {
      println("Iteration " + i)
      if (crash) {
        var crashes = 0
        while (crashes < 10) {
          // crash process sometimes
          val proc = startProg(s"backup$config $backup1 $input".split(" "))
          @volatile var finished = false
          val t = new Thread() {
            override def run() {
              proc.waitFor()
              finished = true
            }
          }
          t.setDaemon(true)
          t.start()
          if (Random.nextBoolean) {
            val secs = Random.nextInt(10) + 2
             l.info(s"Waiting for $secs seconds before destroying process")
            Thread.sleep(secs*1000)
          } else {
             l.info("Waiting for new hashlist before destroying process")
            val checkpoints = numberOfCheckpoints
            while (numberOfCheckpoints == checkpoints || finished) {
              Thread.sleep(100)
            }
          }
          crashes += 1
          proc.destroy()
        }
        l.info("Crashes done, letting process finish now")
      }
      // let backup finish
      startAndWait(s"backup$config $backup1 $input".split(" ")) should be (0)
      // no temp files in backup
      input.listFiles().filter(_.getName().startsWith("temp")).toList should be('empty)
      // verify backup
      startAndWait(s"verify$configRestore --percent-of-files-to-check 50 $backup1".split(" ")) should be (0)
      

      if (redundancy) {
        // Testing what happens when messing with the files
        messupBackupFiles()
      }
      // restore backup to folder, folder already contains old restored files.
      startAndWait(s"restore$configRestore --restore-to-folder $restore1 $backup1".split(" ")) should be (0)
      // compare files
      compareBackups(input, restore1)
      // delete some files
      if (i != iterations) {
        l.info("Changing files")
        fg.changeSome
        l.info("Changing files done")
      }
    }
  }

  def messupBackupFiles() {
    val files = backup1.listFiles()
    val set = Set("hashlists","files")
    files.filter(x => set.exists(x.getName.toLowerCase().startsWith(_))).foreach { f =>
      l.info("Messing up " + f + " length " + f.length())
      val raf = new RandomAccessFile(f, "rw")
      raf.seek(raf.length() / 2);
      raf.write(("\0").getBytes)
      raf.close()
    }
    files.filter(_.getName.startsWith("volume")).filter(_.length > 100*1024).foreach { f =>
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
