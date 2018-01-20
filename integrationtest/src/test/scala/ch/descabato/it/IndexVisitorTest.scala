package ch.descabato.it

import java.io.{File, FileOutputStream, RandomAccessFile}

import ch.descabato.core_old.OldIndexVisitor
import ch.descabato.utils.FileUtils
import ch.descabato.{RichFlatSpecLike, TestUtils}
import org.apache.commons.io.IOUtils
import org.scalatest.{BeforeAndAfter, FlatSpec, Ignore}

import scala.util.Random

@Ignore
class IndexVisitorTest extends IntegrationTestBase with BeforeAndAfter {

  val file = new File("ignorelist.txt")
  val destDir = new File(baseFolder, "indextests")

  before {
    FileUtils.deleteAll(destDir)
  }

  def setupIgnore(ignores: String *) {
    val fi = new FileOutputStream(file)
    for (ignore <- ignores) {
      IOUtils.write(ignore, fi)
    }
    IOUtils.closeQuietly(fi)
  }

  def setupRandomFiles() = {
    val fg = new FileGen(destDir, "2 Mb", minFiles = 3)
    fg.generateFiles()
    fg
  }

  "index visitor" should "ignore all files for pattern **" in {
    setupIgnore("**")
    setupRandomFiles()
    assert(destDir.listFiles().length > 0)
    val visitor = new OldIndexVisitor(Map.empty, Some(file), recordNew = true, recordAll = true)
    visitor.walk(destDir::Nil)
    assert(visitor.newDesc.size === 0)
  }

  it should "ignore all copy files for pattern copy*" in {
    setupIgnore("copy*")
    val fileGen = setupRandomFiles()
    assert(destDir.listFiles().toVector.exists(_.getName.contains("copy")))
    val visitor = new OldIndexVisitor(Map.empty, Some(file), recordNew = true, recordAll = true)
    visitor.walk(destDir::Nil)
    assert(fileGen.folderList.size === visitor.newDesc.folders.size)
    assert(fileGen.fileList.size - 3 === visitor.newDesc.files.size)
    assert(visitor.newDesc.allParts.filter(_.path.contains("copy")).isEmpty)
  }

  it should "find all files and folders with no ignore pattern set" in {
    val fileGen = setupRandomFiles()
    runFirstIteration(fileGen)
  }

  it should "detect everything as unchanged with no modifications" in {
    val fileGen = setupRandomFiles()
    val visitor1 = runFirstIteration(fileGen)
    val visitor2 = new OldIndexVisitor(visitor1.newDesc.asMap, None, recordNew = true, recordUnchanged = true)
    visitor2.walk(destDir::Nil)
    assert(fileGen.folderList.size === visitor2.unchangedDesc.folders.size)
    assert(fileGen.fileList.size === visitor2.unchangedDesc.files.size)
    assert(visitor2.newDesc.allParts.isEmpty)
  }

  it should "detect modification date changes" in {
    val fileGen = setupRandomFiles()
    val visitor1 = runFirstIteration(fileGen)
    fileGen.fileList.head.setLastModified(System.currentTimeMillis() + 1000)
    fileGen.folderList.head.setLastModified(System.currentTimeMillis() + 1000)
    val visitor2 = new OldIndexVisitor(visitor1.newDesc.asMap, None, recordNew = true, recordUnchanged = true)
    visitor2.walk(destDir::Nil)
    assert(fileGen.folderList.size - 1 === visitor2.unchangedDesc.folders.size)
    assert(fileGen.fileList.size - 1 === visitor2.unchangedDesc.files.size)
    assert(fileGen.fileList.head.getAbsolutePath === visitor2.newDesc.files.head.path)
    assert(fileGen.folderList.head.getAbsolutePath === visitor2.newDesc.folders.head.path)
  }

  it should "detect file size changes" in {
    val fileGen = setupRandomFiles()
    val visitor1 = runFirstIteration(fileGen)
    val changeSize = fileGen.fileList.filter(_.length() > 100).head
    val raf = new RandomAccessFile(changeSize, "rw")
    raf.setLength(changeSize.length() - 1)
    raf.close()
    val visitor2 = new OldIndexVisitor(visitor1.newDesc.asMap, None, recordNew = true, recordUnchanged = true)
    visitor2.walk(destDir::Nil)
    assert(fileGen.folderList.size === visitor2.unchangedDesc.folders.size)
    assert(fileGen.fileList.size - 1 === visitor2.unchangedDesc.files.size)
    assert(changeSize.getAbsolutePath === visitor2.newDesc.files.head.path)
  }

  it should "detect file to folder changes" in {
    val fileGen = setupRandomFiles()
    val visitor1 = runFirstIteration(fileGen)
    val changeToFolder = fileGen.fileList.head
    changeToFolder.delete()
    changeToFolder.mkdir()
    val visitor2 = new OldIndexVisitor(visitor1.newDesc.asMap, None, recordNew = true, recordUnchanged = true)
    visitor2.walk(destDir::Nil)
    assert(fileGen.fileList.size - 1 === visitor2.unchangedDesc.files.size)
    // parent directory may or may not be seen as changed (due to modification date change), important is the new one
    assert(visitor2.newDesc.folders.map(_.path).contains(changeToFolder.getAbsolutePath))
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
