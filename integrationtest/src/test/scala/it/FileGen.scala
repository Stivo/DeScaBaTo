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
import java.io.OutputStream

class FileGen(val folder: File, maxSize: String = "20Mb", minFiles: Int = 10) extends TestUtils {

  var random = new Random()
  val maxSizeIn = Size(maxSize).bytes.toInt
  var maxFiles = 1000
  var maxFolders = 50
  val subfolderChance = 20
  var folderList = Buffer[File]()
  var fileList = Buffer[File]()
  val folderLevels = Map[File, Int]()

  def times(n: Int)(f: => Unit) {
    (1 to n).foreach(_ => f)
  }

  def generateFiles() {
    deleteAll(folder)
    folder.mkdir()
    folderList += folder
    times(25) { newFolder() }
    var bigFiles = 4
    newFile(maxSizeIn.toInt / 2)
    val copyFrom1 = fileList.last
    newFile(maxSizeIn.toInt / 10)
    while (fileList.size < minFiles || fileList.map(_.length).sum < maxSizeIn) {
      newFile(1000000)
    }
    times(10)(newFile(0))
    Files.copy(copyFrom1.toPath, new File(folder, "copy1").toPath)
    Files.copy(selectFile.toPath, new File(folder, "copy2").toPath)
    Files.copy(selectFile.toPath, new File(folder, "copy3").toPath)
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
    val folders = random.nextInt(10) + 2
    val int = random.nextInt(100) + 25
    times(folders)(newFolder())
    times(int)(newFile(100000))
    times(25)(changeFile)
    times(10)(renameFile)
    times(5)(deleteFile)
  }

  implicit class MoreRandom(r: Random) {
    def nextBytes(x: Long) = {
      val buf = Array.ofDim[Byte](random.nextInt(x.toInt))
      r.nextBytes(buf)
      buf
    }
  }

  def renameFile() {
    if (fileList.size < 10) {
      times(10)(newFile(10000))
    }
    val file = selectFolderOrFile
    val fileNew = new File(file.getParentFile(), generateName())
    Files.move(file.toPath(), fileNew.toPath())
    l.info("File was moved from "+file+" to "+fileNew)
    rescan
  }

  def deleteFile() {
    var i = 0
    var lastfile: File = selectFolderOrFile
    while (i < 5 && lastfile.exists()) {
      lastfile = selectFolderOrFile()
      lastfile.delete()
      i += 1
    }
    l.info("DeleteFile ran "+i+" times and deleted "+lastfile.exists+" the file "+lastfile)
    rescan
  }

  def changeFile() {
    val file = selectFile()
    val raf = new RandomAccessFile(file, "rw")
    try {
      if (raf.length() < 100) {
        raf.write(random.nextBytes(100000))
      } else {
        raf.seek(random.nextInt(raf.length().toInt / 2))
        raf.write(random.nextBytes(raf.length() - raf.getFilePointer()))
      }
    } finally {
      raf.close()
    }
    l.info("File was changed "+file)
  }

  def select[T](x: Seq[T]) = x.size match {
    case 0 => throw new IllegalArgumentException("Not possible")
    case i if i >= 1 => x(random.nextInt(i))
  }
  
  def selectFile() = {
    if (fileList.isEmpty) {
      rescan
    }
    while(fileList.isEmpty) {
      newFile()
    }
    select(fileList)
  }
  
  def selectFolder() = {
    if (folderList.isEmpty) {
      rescan
    }
    while (folderList.isEmpty) {
      val f = new File(folder, generateName)
      f.mkdirs()
      if (f.exists() && f.isDirectory())
    	folderList += f
    }
    select(folderList)
  }
  
  def selectFolderOrFile() = {
    select(List(selectFile, selectFolder)) 
  }

  def generateName() = random.alphanumeric.take(15).mkString ++ random.nextString(3)

  def newFile(maxSize: Int = 100000) {
    var done = false
    while (!done) {
      val name = new File(selectFolder, generateName)
      if (name.exists()) {
        if (!fileList.contains(name))
          fileList += name
        return
      }
      var fos: OutputStream = null
      try {
        fos = new FileOutputStream(name)
        if (maxSize > 0) {
          val buf = Array.ofDim[Byte](random.nextInt(maxSize) + maxSize / 10)
          var chance = 1.0
          while (random.nextFloat < chance) {
            random.nextBytes(buf)
            fos.write(buf)
            chance = chance / 1.5
          }
        }
        fos.close()
        l.info("New file created "+name)
        fileList += name
        done = true
        Thread.sleep(random.nextInt(100))
      } catch {
        case io: IOException => // ignore, doesnt matter
      } finally {
        if (fos != null)
          fos.close()
      }
    }
  }

  def newFolder(baseIn: Option[File] = None) {
    val base = selectFolder
    
    var name: File = null
    do {
      name = new File(base, generateName)
      name.mkdir()
      
    } while (!name.exists())
    l.info("folder was created "+name)
    folderList += name
  }

}
