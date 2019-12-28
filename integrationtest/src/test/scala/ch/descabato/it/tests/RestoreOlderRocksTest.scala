package ch.descabato.it

import java.io.File
import java.io.FileOutputStream
import java.nio.file.Files
import java.util.stream.Collectors

import ch.descabato.core.config.BackupFolderConfiguration
import ch.descabato.core.model.Size
import ch.descabato.rocks.MetadataImporter
import ch.descabato.rocks.RocksEnv
import org.apache.commons.io.FileUtils
import org.apache.commons.io.IOUtils
import org.scalatest.Matchers._

import scala.collection.JavaConverters._

class RestoreOlderRocksTest extends IntegrationTestBase with RocksIntegrationTest {

  var input1 = folder("input_old1")
  var input2 = folder("input_old2")
  var input3 = folder("input_old3")
  val backup1 = folder("backup_old1")
  val restore1 = folder("restore_old1")
  val restore2 = folder("restore_old2")
  val restore3 = folder("restore_old3")

  val restoreInfoName = "restore-info.txt"

  lazy val fg = new FileGen(input3, "20Mb")
  val ignoreFile = new File("ignored.txt")
  var part = 0

  def reportFiles(): Unit = {
    Files.walk(backup1.toPath).collect(Collectors.toList()).asScala.sorted.foreach { f =>
      val path = backup1.toPath.relativize(f).toString
      println(s"$path ${Size(f.toFile.length())}")
    }
    DumpRocksdb.main(Array(backup1.getAbsolutePath))
  }

  "restore old versions" should "setup" in {
    deleteAll(input1, input2, input3, backup1, restore1, restore2, restore3)
    fg.rescan()
    fg.generateFiles()
    fg.rescan()

    val fos = new FileOutputStream(ignoreFile)
    IOUtils.write("copy1", fos)
    IOUtils.closeQuietly(fos)
  }

  it should "backup 1/3" in {
    startAndWait(s"backup --compression gzip $backup1 $input3".split(" ")) should be(0)
  }

  it should getNextPartName() in {
    FileUtils.copyDirectory(input3, input1, true)
    fg.changeSome()
    reportFiles
  }

  it should "delete rocks data and restore it" in {
    deleteAll(new File(backup1, "rocks"))
    val env = RocksEnv(BackupFolderConfiguration(backup1), false)
    val importer = new MetadataImporter(env)
    importer.importMetadata()
    reportFiles()
    env.close()
  }

  it should "backup 2/3" in {
    startAndWait(s"backup --ignore-file ${ignoreFile.getAbsolutePath} $backup1 $input3".split(" ")) should be(0)
  }

  it should getNextPartName() in {
    FileUtils.copyDirectory(input3, input2, true)
    fg.changeSome()
    reportFiles
  }

  it should "backup 3/3 after rocksdb has been lost" in {
    startAndWait(s"backup $backup1 $input3".split(" ")) should be(0)
  }

  var backupRevisions = 1 to 3

  it should getNextPartName() in {
    reportFiles
  }

  it should "restore 1/3" in {
    startAndWait(s"restore --restore-backup ${backupRevisions(0)} --restore-info $restoreInfoName --restore-to-folder $restore1 $backup1".split(" ")) should be(0)
    //    assertRestoreInfoWritten(restore1, backupFiles(0))
    compareBackups(input1, restore1)
  }

  it should "restore 2/3" in {
    startAndWait(s"restore --restore-backup ${backupRevisions(1)} --restore-to-folder $restore2 $backup1".split(" ")) should be(0)
    //    FileUtils.deleteQuietly(new File(input2, "copy1"))
    compareBackups(input2, restore2)
  }

  it should "restore 3/3" in {
    startAndWait(s"restore --restore-info $restoreInfoName --restore-to-folder $restore3 $backup1".split(" ")) should be(0)
    //    assertRestoreInfoWritten(restore3, backupFiles(2))
    FileUtils.deleteQuietly(new File(input3, "copy1"))
    compareBackups(input3, restore3)
  }

  private def getNextPartName() = {
    s"prepare next test ${part = part + 1; part}"
  }

  def assertRestoreInfoWritten(folder: File, filename: String) {
    //    val reader = new FileReader(new File(folder, restoreInfoName))
    //    val string = IOUtils.toString(reader)
    //    IOUtils.closeQuietly(reader)
    //    assert(string.contains(filename))
  }

}
