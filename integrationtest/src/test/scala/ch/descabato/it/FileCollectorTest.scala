package ch.descabato.it

import java.io.{File, FileOutputStream}

import ch.descabato.core.FileVisitorCollector
import ch.descabato.core_old.OldIndexVisitor
import ch.descabato.frontend.MaxValueCounter
import ch.descabato.utils.FileUtils
import org.apache.commons.io.IOUtils
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}

class FileCollectorTest extends IntegrationTestBase with BeforeAndAfter with BeforeAndAfterAll {

  val file = new File("ignorelist.txt")
  val destDir = new File(baseFolder, "indextests")

  before {
    FileUtils.deleteAll(destDir)
  }

  override protected def afterAll(): Unit = {
    FileUtils.deleteAll(destDir)
  }

  def setupIgnore(ignores: String *) {
    val fi = new FileOutputStream(file)
    for (ignore <- ignores) {
      IOUtils.write(ignore, fi)
    }
    IOUtils.closeQuietly(fi)
  }

  class DummyCounter extends MaxValueCounter {
    override def name: String = "dummy"
  }

  def setupRandomFiles() = {
    val fg = new FileGen(destDir, "2 Mb", minFiles = 3)
    fg.generateFiles()
    fg
  }

  "file collector" should "ignore all files for pattern **" in {
    setupIgnore("**")
    setupRandomFiles()
    assert(destDir.listFiles().length > 0)
    val collector = new FileVisitorCollector(Some(file), new DummyCounter, new DummyCounter)
    collector.walk(destDir.toPath)
    assert(collector.dirs.isEmpty)
    assert(collector.files.isEmpty)
  }

  it should "ignore all copy files for pattern copy*" in {
    setupIgnore("copy*")
    val fileGen = setupRandomFiles()
    assert(destDir.listFiles().toVector.exists(_.getName.contains("copy")))
    val collector = new FileVisitorCollector(Some(file), new DummyCounter, new DummyCounter)
    collector.walk(destDir.toPath)
    assert(fileGen.folderList.size === collector.dirs.size)
    assert(fileGen.fileList.size - 3 === collector.files.size)
    assert(collector.files.filter(_.toAbsolutePath.toString.contains("copy")).isEmpty)
  }

  it should "find all files and folders with no ignore pattern set" in {
    val fileGen = setupRandomFiles()
    runFirstIteration(fileGen)
  }

  def runFirstIteration(fileGen: FileGen) = {
    assert(destDir.listFiles().length > 0)
    val visitor1 = new OldIndexVisitor(Map.empty, None, recordNew = true, recordAll = true, recordUnchanged = true)
    visitor1.walk(destDir::Nil)
    assert(fileGen.folderList.size === visitor1.newDesc.folders.size)
    assert(fileGen.fileList.size === visitor1.newDesc.files.size)
    visitor1
  }

}
