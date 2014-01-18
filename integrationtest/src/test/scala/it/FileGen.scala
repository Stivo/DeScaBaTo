package ch.descabato.it

import ch.descabato.Size
import java.io.File
import scala.util.Random
import scala.collection.mutable.Buffer
import java.io.FileOutputStream
import java.nio.file.Files
import java.io.IOException
import ch.descabato.OldIndexVisitor
import scala.collection.mutable
import java.io.RandomAccessFile
import ch.descabato.TestUtils

class FileGen(val folder : File) extends TestUtils {

  var random = new Random(255)
  var maxSize = Size("500MB")
  var maxFiles = 1000
  var maxFolders = 50
  val subfolderChance = 20
  var folderList = Buffer[File]()
  var fileList = Buffer[File]()
  val folderLevels = Map[File, Int]()

  def generateFiles() {
    deleteAll(folder)
    folder.mkdir()
    newFolder(folder)
    var bigFiles = 4
    newFile(maxSize.bytes.toInt / 2)
    val copyFrom1 = fileList.last
    newFile(maxSize.bytes.toInt / 10)
    while (fileList.map(_.length).sum < maxSize.bytes) {
      newFile(random.nextInt(1000000))
    }
    Files.copy(copyFrom1.toPath, new File(folder, "copy1").toPath)
    Files.copy(select(fileList).toPath, new File(folder, "copy2").toPath)
    Files.copy(select(fileList).toPath, new File(folder, "copy3").toPath)
  }

  def rescan() {
    val index = new OldIndexVisitor(mutable.Map.empty, recordAll = true)
    Files.walkFileTree(folder.toPath(), index)
    val (folders, files) = index.all.map(x => new File(x.path)).partition(_.isDirectory())
    folderList.clear
    folderList ++= folders
    fileList.clear
    fileList ++= files
  }
  
  def changeSome() {
    val folders = random.nextInt(10)+2
    val int = random.nextInt(100)+5
    (0 to folders).foreach(_ => newFolder(select(folderList)))
    (0 to int).foreach(_ => newFile(100000))
    (0 to random.nextInt(50)).foreach(_ => changeFile)
    (0 to random.nextInt(20)).foreach(_ => renameFile)
    (0 to random.nextInt(10)).foreach(_ => deleteFile)
  }
  
  implicit class MoreRandom(r: Random) {
    def nextBytes(x: Long) = {
      val buf = Array.ofDim[Byte](random.nextInt(x.toInt))
      r.nextBytes(buf)
      buf
    }
  }
  
  def renameFile() {
    var done = false
	while (!done) {
	  val file = select(fileList++folderList)
	  val fileNew = new File(file.getParentFile(), generateName())
	  done = file.renameTo(fileNew)
	}
    rescan
  }

  def deleteFile() {
	while (!select(fileList++folderList).delete()) {}
	rescan
  }
  
  def changeFile() {
    val file = select(fileList)
    val raf = new RandomAccessFile(file, "rw")
    raf.seek(random.nextInt(raf.length().toInt/2))
    raf.write(random.nextBytes(raf.length()-raf.getFilePointer()))
    raf.close()
  }
  
  def select[T](x: Seq[T]) = x(random.nextInt(x.size - 1))

  def generateName() = random.alphanumeric.take(15).mkString ++ random.nextString(3)

  def newFile(maxSize: Int = 100000) {
    var done = false
    while (!done) {
      val name = new File(select(folderList), generateName)
      if (name.exists()) {
        fileList += name
        return
      }
      try {
        val fos = new FileOutputStream(name)
        val buf = Array.ofDim[Byte](random.nextInt(maxSize) + maxSize / 10)
        var chance = 1.0
        while (random.nextFloat < chance) {
          random.nextBytes(buf)
          fos.write(buf)
          chance = chance / 1.5
        }
        fos.close()
        fileList += name
        done = true
        Thread.sleep(random.nextInt(100))
      } catch {
        case io: IOException => println(io.getMessage)
      }
    }
  }

  def newFolder(base: File) {
    val name = new File(base, generateName)
    name.mkdir()
    if (name.exists)
      folderList += name
    if (folderList.size < maxFolders) {
      if (random.nextInt(100) > subfolderChance) {
        newFolder(base)
      } else {
        newFolder(select(folderList))
      }
    }
  }

}
