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

class IntegrationTest extends FlatSpec with BeforeAndAfter with GeneratorDrivenPropertyChecks with TestUtils {
  import org.scalacheck.Gen._

  CLI._overrideRunsInJar = true
  CLI.testMode = true
  //ConsoleManager.testSetup
  AES.testMode = true

  var baseFolder = new File("integrationtest/testdata")

  var input = new File(baseFolder, "input")

  def folder(s: String) = new File(baseFolder, s)
  val backup1 = folder("backup1")
  val restore1 = folder("restore1")

  "plain backup" should "work" in {
    testWith("", "", 5)
  }

  "encrypted backup" should "work" in {
    testWith(" --passphrase mypassword", " --passphrase mypassword", 3)
  }

  "low volumesize backup" should "work" in {
    testWith(" --volume-size 1Mb --block-size 2Kb", "", 3)
  }

  "backup with crashes" should "work" in {
    testWith(" --volume-size 10Mb", "", 5, true)
  }

  "backup with multiple threads" should "work" in {
    testWith(" --threads 4 --volume-size 20Mb", "", 5, false)
  }
  
  def testWith(config: String, configRestore: String, iterations: Int, crash: Boolean = false) {
    println(baseFolder.getAbsolutePath())
    baseFolder.mkdirs()
    assume(baseFolder.getAbsoluteFile().exists())
    deleteAll(backup1)
    deleteAll(restore1)
    // create some files
    val fg = new FileGen(input)
    fg.generateFiles
    fg.rescan()
    for (i <- 1 to iterations) {
      println("Iteration " + i)
      if (crash) {
        // crash process sometimes
        val t = new Thread() {
          override def run() {
            CLI.main(s"backup$config $backup1 $input".split(" "))
          }
        }
        t.start()
        Thread.sleep(Random.nextInt(7000) + 3000)
        t.stop()
        Streams.closeAll
      }
      // let backup finish
      CLI.main(s"backup$config $backup1 $input".split(" "))
      // no temp files in backup
      input.listFiles().filter(_.getName().startsWith("temp")).toList should be('empty)
      // verify backup
      CLI.main(s"verify$configRestore --percent-of-files-to-check 50 $backup1".split(" "))
      CLI.lastErrors should be (0)
      // restore backup to folder, folder already contains old restored files.
      CLI.main(s"restore$configRestore --restore-to-folder $restore1 --relative-to-folder $input $backup1".split(" "))
      // compare files
      compareBackups(input, restore1)
      // delete some files
      if (i != iterations) {
        println("Changing files")
        fg.changeSome
        println("Changing files done")
      }
    }
    // delete restored parts
    // check files of backup are same in restore
    // mess up backup
    // check that restore fails
  }

  after {
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
        assert(c1.lastModified === c2.lastModified, "Last modified should be the same")
        assert(c1.getName === c2.getName, "name should be same for " + c1)
        assert(c1.length === c2.length, "length should be same for " + c1)
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
