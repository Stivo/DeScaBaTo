package ch.descabato.it

import java.io.File
import java.util.{List => JList}

import org.scalatest.Matchers._
import org.scalatest._
import org.scalatest.prop.GeneratorDrivenPropertyChecks

import scala.util.Random

class IntegrationTest extends IntegrationTestBase with BeforeAndAfter with BeforeAndAfterAll with GeneratorDrivenPropertyChecks {

  var input = folder("input1")
  val backup1 = folder("backup1")
  val restore1 = folder("restore1")

  override def beforeAll {
    val destfile = new File(descabatoFolder, "core/target/scala-2.10/jacoco/jacoco.exec")
    destfile.delete()
    System.setProperty("logname", "integrationtest.log")
    deleteAll(logFolder)
    logFolder.mkdirs()
  }

  testWith("plain backup", " --threads 1", "", 1, "10Mb")

  testWith("encrypted backup", " --threads 5 --compression none --volume-size 20Mb --passphrase mypassword", " --passphrase mypassword", 2, "20Mb")

//    "backup with redundancy" should "recover" in {
//      testWith(" --volume-size 10mb", "", 1, "20Mb", false, true)
//    }

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
      deleteAll(input, backup1, restore1)
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
        messupBackupFiles(backup1)
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
        messupBackupFiles(backup1)
        l.info("Verification should fail after files have been messed up")
        startAndWait(s"verify$configRestore --percent-of-files-to-check 100 $backup1".split(" "), false) should not be (0)
      }
    }
  }

}
