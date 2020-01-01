package ch.descabato.it.rocks

import java.io.File
import java.io.FileOutputStream
import java.nio.file.Files
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneId
import java.util.stream.Collectors

import ch.descabato.core.config.BackupFolderConfiguration
import ch.descabato.core.model.Size
import ch.descabato.core.util.FileManager
import ch.descabato.it.DumpRocksdb
import ch.descabato.it.FileGen
import ch.descabato.it.IntegrationTestBase
import org.apache.commons.io.FileUtils
import org.apache.commons.io.IOUtils
import org.scalatest.Matchers._

import scala.collection.JavaConverters._

class UpgradeRocksTest extends IntegrationTestBase with RocksIntegrationTest {

  private var _mainClass: String = oldMainClass

  override def mainClass: String = _mainClass

  var input1 = folder("input_old1")
  var input2 = folder("input_old2")
  var input3 = folder("input_old3")
  val backup1 = folder("backup_old1")
  val restore1 = folder("restore_old1")
  val restore2 = folder("restore_old2")
  val restore2_2 = folder("restore_old2_2")
  val restore3 = folder("restore_old3")

  val restoreInfoName = "restore-info.txt"

  lazy val fg = new FileGen(input3, "10Mb")
  val ignoreFile = new File("ignored.txt")
  var part = 0
  val fm = new FileManager(BackupFolderConfiguration(backup1))

  def reportFiles(): Unit = {
    Files.walk(backup1.toPath).collect(Collectors.toList()).asScala.sorted.foreach { f =>
      val path = backup1.toPath.relativize(f).toString
      val date = LocalDateTime.ofInstant(Instant.ofEpochMilli(f.toFile.lastModified()), ZoneId.systemDefault()).toString
      println(f"${Size(f.toFile.length())}%10s ${date}%s $path%s")
    }
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

  it should "backup 1/4" in {
    startAndWait(s"backup --compression gzip $backup1 $input3".split(" ")) should be(0)
  }

  it should getNextPartName() in {
    FileUtils.copyDirectory(input3, input1, true)
    fg.changeSome()
    reportFiles()
  }

  it should "backup 2/4" in {
    startAndWait(s"backup --ignore-file ${ignoreFile.getAbsolutePath} $backup1 $input3".split(" ")) should be(0)
  }

  def reportDbContent(): Unit = {
    DumpRocksdb.main(Array(backup1.getAbsolutePath))
  }

  var volumesAfterBackup2 = 0

  it should getNextPartName() in {
    //    FileUtils.copyDirectory(input3, input2, true)
    //    fg.changeSome()
    reportFiles()
    volumesAfterBackup2 = fm.volume.getFiles().length
  }

  it should "switch version to rocks based code" in {
    _mainClass = newMainClass
  }

  it should "refuse to backup if the version was upgraded" in {
    startAndWait(s"backup $backup1 $input3".split(" ")) should not be (0)
  }

  it should "upgrade correctly" in {
    reportFiles()
    startAndWait(s"upgrade $backup1".split(" ")) should be(0)
    reportFiles()
    reportDbContent()
  }

  it should "change all last modified dates" in {
    val newLastModified = System.currentTimeMillis()
    Files.walk(input3.toPath).forEach { f =>
      f.toFile.setLastModified(newLastModified)
    }
    reportFiles()
  }

  it should "backup 3/4 after upgrade without adding new volumes" in {
    startAndWait(s"backup $backup1 $input3".split(" ")) should be(0)
    fm.volume.getFiles().length should be(volumesAfterBackup2)
  }

  it should "backup 4/4 after upgrade with adding new volumes" in {
    //    startAndWait(s"backup $backup1 $input2".split(" ")) should be(0)
  }

  //
  //  it should "not change the files that were declared finished in rocksdb" in {
  //    fm.dbexport.fileForNumber(0, temp = true) should not(exist)
  //    fm.dbexport.fileForNumber(0, temp = false) should (exist)
  //    fm.dbexport.fileForNumber(0, temp = false).lastModified() should be(dbExportModified)
  //    fm.volume.fileForNumber(0, temp = true) should not(exist)
  //    fm.volume.fileForNumber(0, temp = false) should (exist)
  //    fm.volume.fileForNumber(0, temp = false).lastModified() should be(volumeModified)
  //  }
  //
  //
  //  it should getNextPartName() in {
  //    fm.volume.getFiles().length should be(volumesAfterBackup2)
  //    reportFiles()
  //    reportDbContent()
  //  }
  //
  //  it should "backup 4/4" in {
  //    startAndWait(s"backup $backup1 $input3".split(" ")) should be(0)
  //  }
  //
  //  it should getNextPartName() in {
  //    reportFiles()
  //    reportDbContent()
  //  }
  //
  //  // ----------------------------------
  //  // Restoring
  //  // ----------------------------------
  //  var backupRevisions = 1 to 4
  //
  //  it should "restore 1/4" in {
  //    startAndWait(s"restore --restore-backup ${backupRevisions(0)} --restore-info $restoreInfoName --restore-to-folder $restore1 $backup1".split(" ")) should be(0)
  //    //    assertRestoreInfoWritten(restore1, backupFiles(0))
  //    compareBackups(input1, restore1)
  //  }
  //
  //  it should "restore 2/4" in {
  //    startAndWait(s"restore --restore-backup ${backupRevisions(1)} --restore-to-folder $restore2 $backup1".split(" ")) should be(0)
  //    //    FileUtils.deleteQuietly(new File(input2, "copy1"))
  //    compareBackups(input2, restore2)
  //  }
  //
  //  it should "restore 3/4" in {
  //    startAndWait(s"restore --restore-backup ${backupRevisions(2)} --restore-to-folder $restore2_2 $backup1".split(" ")) should be(0)
  //    //    assertRestoreInfoWritten(restore3, backupFiles(2))
  //    FileUtils.deleteQuietly(new File(input3, "copy1"))
  //    compareBackups(input2, restore2)
  //  }
  //
  //  it should "restore 4/4" in {
  //    startAndWait(s"restore --restore-info $restoreInfoName --restore-to-folder $restore3 $backup1".split(" ")) should be(0)
  //    //    assertRestoreInfoWritten(restore3, backupFiles(2))
  //    FileUtils.deleteQuietly(new File(input3, "copy1"))
  //    compareBackups(input3, restore3)
  //  }
  //
  private def getNextPartName() = {
    s"prepare next test ${part = part + 1; part}"
  }

  //
  //  def assertRestoreInfoWritten(folder: File, filename: String) {
  //    //    val reader = new FileReader(new File(folder, restoreInfoName))
  //    //    val string = IOUtils.toString(reader)
  //    //    IOUtils.closeQuietly(reader)
  //    //    assert(string.contains(filename))
  //  }

}
