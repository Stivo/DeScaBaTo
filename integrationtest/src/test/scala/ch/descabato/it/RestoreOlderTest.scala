package ch.descabato.it

import org.apache.commons.io.FileUtils
import org.scalatest.Matchers._

class RestoreOlderTest extends IntegrationTestBase {

  var input1 = folder("input_old1")
  var input2 = folder("input_old2")
  var input3 = folder("input_old3")
  val backup1 = folder("backup_old1")
  val restore1 = folder("restore_old1")
  val restore2 = folder("restore_old2")
  val restore3 = folder("restore_old3")

  "restore old versions " should "work" in {
    deleteAll(input1, input2, input3, backup1, restore1, restore2, restore3)
    val fg = new FileGen(input3, "20Mb")
    fg.rescan()
    fg.generateFiles()
    fg.rescan()

    startAndWait(s"backup --compression gzip $backup1 $input3".split(" ")) should be(0)
    FileUtils.copyDirectory(input3, input1, true)
    fg.changeSome()
    startAndWait(s"backup $backup1 $input3".split(" ")) should be(0)
    FileUtils.copyDirectory(input3, input2, true)
    fg.changeSome()
    startAndWait(s"backup $backup1 $input3".split(" ")) should be(0)
    val backupFiles = backup1.listFiles().filter(_.getName.startsWith("backup_")).map(_.getName()).toList.sorted
    startAndWait(s"restore --restore-backup ${backupFiles(0)} --restore-to-folder $restore1 $backup1".split(" ")) should be(0)
    compareBackups(input1, restore1)
    startAndWait(s"restore --restore-backup ${backupFiles(1)} --restore-to-folder $restore2 $backup1".split(" ")) should be(0)
    compareBackups(input2, restore2)
    startAndWait(s"restore --restore-backup ${backupFiles(2)} --restore-to-folder $restore3 $backup1".split(" ")) should be(0)
    compareBackups(input3, restore3)
  }

}
