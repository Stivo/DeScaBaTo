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

class IntegrationTest extends FlatSpec with BeforeAndAfter with GeneratorDrivenPropertyChecks with TestUtils {
  import org.scalacheck.Gen._

  CLI._overrideRunsInJar = true
  CLI.testMode = true
  //ConsoleManager.testSetup

  var baseFolder = new File("integrationtest/testdata")

  var input = new File(baseFolder, "input")

  def folder(s: String) = new File(baseFolder, s)
  val backup1 = folder("backup1")
  val restore1 = folder("restore1")

  before {
    Par2Handler.TESTONLY_reset()
    deleteAll(input)
    deleteAll(backup1)
    deleteAll(restore1)
  }
  
  "plain backup" should "work" in {
    testWith(" --no-redundancy", "", 5, "300Mb")
  }

  "encrypted backup" should "work" in {
    testWith(" --compression gzip --volume-size 20Mb --no-redundancy --passphrase mypassword", " --passphrase mypassword", 3, "50Mb")
  }

  "low volumesize backup with prefix" should "work" in {
    testWith(" --compression bzip2 --no-redundancy --prefix testprefix --volume-size 1Mb --block-size 2Kb", " --prefix testprefix", 2, "20Mb")
  }

  "backup with multiple threads" should "work" in {
    testWith(" --no-redundancy --threads 4 --volume-size 20Mb", "", 5, "50Mb", false)
  }

  "backup with redundancy" should "recover" in {
    testWith(" --volume-size 10mb", "", 1, "20Mb", false, true)
  }
  
  //  "backup with crashes" should "work" in {
  //    testWith(" --checkpoint-every 10Mb --volume-size 10Mb", "", 5, true)
  //  }

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
        while (crashes < 5) {
          // crash process sometimes
          val t = new Thread() {
            override def run() {
              CLI.main(s"backup$config $backup1 $input".split(" "))
            }
          }
          t.setDaemon(true)
          t.start()
          Thread.sleep(Random.nextInt(2000) + 2000)
          if (!t.isAlive()) {
            crashes = 5
          }
          t.stop()
          Streams.closeAll
        }
      }
      // let backup finish
      CLI.main(s"backup$config $backup1 $input".split(" "))
      // no temp files in backup
      input.listFiles().filter(_.getName().startsWith("temp")).toList should be('empty)
      // verify backup
      CLI.main(s"verify$configRestore --percent-of-files-to-check 50 $backup1".split(" "))
      CLI.lastErrors should be(0)

      if (redundancy) {
        // Testing what happens when messing with the files
        messupBackupFiles()
      }
      // restore backup to folder, folder already contains old restored files.
      CLI.main(s"restore$configRestore --restore-to-folder $restore1 $backup1".split(" "))
      // compare files
      compareBackups(input, restore1)
      // delete some files
      if (i != iterations) {
        l.info("Changing files")
        fg.changeSome
        l.info("Changing files done")
      }
    }
    // delete restored parts
    // check files of backup are same in restore
    // mess up backup
    // check that restore fails
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
