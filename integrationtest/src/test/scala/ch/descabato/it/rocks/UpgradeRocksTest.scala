package ch.descabato.it.rocks

import ch.descabato.it.IntegrationTestBase

@deprecated
// TODO delete this as well
class UpgradeRocksTest extends IntegrationTestBase with RocksIntegrationTest {

  //  /*
  //  create input3 with some files
  //  backup input3 with old version
  //  copy input 3 to input 1
  //  change some files in input 3
  //  backup input3 with old version again
  //  try backing up with new version
  //  upgrade
  //  touch all files in input3
  //  copy all files in input3 to to input2
  //  backup with new version => no new volume created
  //  change files in input 3
  //  backup with new version again
  //  restore revision 1 and compare with input 1
  //  restore revision 3 and compare with input 2
  //  restore revision 4 and compare with input 3
  //   */
  //
  //  private var _mainClass: String = oldMainClass
  //
  //  override def mainClass: String = _mainClass
  //
  //
  //  var input1 = folder("input_old1")
  //  var input2 = folder("input_old2")
  //  var input3 = folder("input_old3")
  //  val backup1 = folder("backup_old1")
  //  val restore1 = folder("restore_old1")
  //  val restore2 = folder("restore_old2")
  //  val restore2_2 = folder("restore_old2_2")
  //  val restore3 = folder("restore_old3")
  //
  //  val restoreInfoName = "restore-info.txt"
  //
  //  lazy val fg = new FileGen(input3, "20mb")
  //  var part = 0
  //  val fm = new FileManager(BackupFolderConfiguration(backup1))
  //
  //  def reportFiles(): Unit = {
  //    Files.walk(backup1.toPath).collect(Collectors.toList()).asScala.sorted.foreach { f =>
  //      val path = backup1.toPath.relativize(f).toString
  //      val date = LocalDateTime.ofInstant(Instant.ofEpochMilli(f.toFile.lastModified()), ZoneId.systemDefault()).toString
  //      println(f"${Size(f.toFile.length())}%10s ${date}%s $path%s")
  //    }
  //  }
  //
  //  def reportDbContent(): Unit = {
  //    DumpRocksdb.main(Array(backup1.getAbsolutePath))
  //  }
  //
  //  "restore old versions" should "setup" in {
  //    deleteAll(input1, input2, input3, backup1, restore1, restore2, restore3)
  //    fg.rescan()
  //    fg.generateFiles()
  //    fg.rescan()
  //  }
  //
  //  lazy val totalBackups = 4
  //
  //  it should s"backup 1/$totalBackups" in {
  //    startAndWait(s"backup --compression gzip $backup1 $input3".split(" ")) should be(0)
  //  }
  //
  //  it should getNextPartName() in {
  //    FileUtils.copyDirectory(input3, input1, true)
  //    fg.changeSome()
  //    reportFiles()
  //  }
  //
  //  it should s"backup 2/$totalBackups" in {
  //    startAndWait(s"backup $backup1 $input3".split(" ")) should be(0)
  //  }
  //
  //  var volumesAfterBackup2 = 0
  //
  //  it should getNextPartName() in {
  //    reportFiles()
  //    volumesAfterBackup2 = fm.volume.getFiles().length
  //  }
  //
  //  it should "switch version to rocks based code" in {
  //    _mainClass = newMainClass
  //  }
  //
  //  it should "refuse to backup if the version was upgraded" in {
  //    startAndWait(s"backup $backup1 $input3".split(" ")) should not be (0)
  //  }
  //
  //  it should "upgrade correctly" in {
  //    reportFiles()
  //    startAndWait(s"upgrade $backup1".split(" ")) should be(0)
  //    reportFiles()
  //    reportDbContent()
  //  }
  //
  //  it should "change all last modified dates" in {
  //    val newLastModified = System.currentTimeMillis()
  //    Files.walk(input3.toPath).forEach { f =>
  //      f.toFile.setLastModified(newLastModified)
  //    }
  //    reportFiles()
  //    FileUtils.copyDirectory(input3, input2, true)
  //  }
  //
  //  it should s"backup 3/$totalBackups after upgrade without adding new volumes" in {
  //    startAndWait(s"backup $backup1 $input3".split(" ")) should be(0)
  //    fm.volume.getFiles().length should be(volumesAfterBackup2)
  //  }
  //
  //  it should getNextPartName() in {
  //    fg.changeSome()
  //    reportFiles()
  //    reportDbContent()
  //  }
  //
  //  it should s"backup 4/$totalBackups after upgrade with adding new volumes" in {
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
  //
  //  lazy val totalRestores = 3
  //
  //  it should s"restore 1/$totalRestores" in {
  //    startAndWait(s"restore --restore-backup 0 --restore-info $restoreInfoName --restore-to-folder $restore1 $backup1".split(" ")) should be(0)
  //    compareBackups(input1, restore1)
  //  }
  //
  //  it should s"restore 2/$totalRestores" in {
  //    startAndWait(s"restore --restore-backup 2 --restore-to-folder $restore2 $backup1".split(" ")) should be(0)
  //    compareBackups(input2, restore2)
  //  }
  //
  //  it should s"restore 3/$totalRestores" in {
  //    startAndWait(s"restore --restore-backup 3 --restore-to-folder $restore3 $backup1".split(" ")) should be(0)
  //    compareBackups(input3, restore3)
  //  }
  //
  //  private def getNextPartName() = {
  //    s"prepare next test ${part = part + 1; part}"
  //  }
  //
  //  def assertRestoreInfoWritten(folder: File, filename: String) {
  //    //    val reader = new FileReader(new File(folder, restoreInfoName))
  //    //    val string = IOUtils.toString(reader)
  //    //    IOUtils.closeQuietly(reader)
  //    //    assert(string.contains(filename))
  //  }

}
