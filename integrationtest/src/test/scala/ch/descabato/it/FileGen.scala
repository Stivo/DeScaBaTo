package ch.descabato.it

import java.io._
import java.nio.file.Files

import ch.descabato.TestUtils
import ch.descabato.core.FileVisitorCollector
import ch.descabato.core.model.Size
import ch.descabato.utils.Implicits._
import ch.descabato.utils.Utils

import scala.collection.mutable.Buffer
import scala.util.Random

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

  def makeCopyFile(i: Int) = new File(folder, "copy"+i)

  def generateFiles() {
    deleteAll(folder)
    folder.mkdirs()
    folderList += folder
    times(25) { newFolder() }
    newFile(maxSizeIn.toInt / 2, true)
    newFile(maxSizeIn.toInt / 10, true)
    val copyFrom1 = fileList.last
    while (fileList.size < minFiles || fileList.map(_.length).sum < maxSizeIn) {
      newFile(1000000)
    }
    times(10)(newFile(0))
    times(5)(newFile(102400, true))
    Files.copy(copyFrom1.toPath, makeCopyFile(1).toPath)
    def selectSmallFile() = {
      var file = selectFile
      while (file.length > 10*1024*1024) {
        file = selectFile
      }
      file
    }
    Files.copy(selectSmallFile.toPath, makeCopyFile(2).toPath)
    Files.copy(selectSmallFile.toPath, makeCopyFile(3).toPath)
    fileList ++= (for (i <- 1 to 3) yield makeCopyFile(i)).toBuffer
  }

  def rescan() {
    val collector = new FileVisitorCollector(None)
    collector.walk(folder.toPath)
    folderList.clear
    folderList ++= collector.dirs.map(_.toFile)
    fileList.clear
    fileList ++= collector.files.map(_.toFile)
  }

  def changeSome() {
    val folders = random.nextInt(10) + 2
    val int = random.nextInt(20) + 10
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
    var file = folder
    while (file == folder) {
      file = selectFolderOrFile
    }
    val fileNew = new File(file.getParentFile, generateName())
    Files.move(file.toPath(), fileNew.toPath())
    l.debug("File was moved from "+file+" to "+fileNew)
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
    l.debug("DeleteFile ran "+i+" times and deleted "+lastfile.exists+" the file "+lastfile)
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
    l.debug("File was changed "+file)
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

  def generateName(): String = {
    val disallowed = ((0 to 31).map(_.toChar) ++ ("""\/?%*:|"<>""".toSet)).toSet
    while(true) {
      var out = random.alphanumeric.take(15).mkString ++ random.nextString(3)
      if (!out.exists(c => disallowed safeContains c)) {
        return out
      }
    }
    // Can not reach here, but the compiler doesn't know that
    ""
  }

  def newFile(maxSize: Int = 100000, sizeExact: Boolean = false) {
    val isTextFile = random.nextInt(100) < 75
    var done = false
    while (!done) {
      val name = new File(selectFolder, generateName+ (if (isTextFile) ".txt" else ".7z"))
      if (name.exists()) {
        if (!fileList.contains(name))
          fileList += name
        return
      }
      var fos: OutputStream = null
      try {
        fos = new FileOutputStream(name)
        if (maxSize > 0) {
          var written = 0
          var chance = 1.0
          lazy val pattern = (0 to 122).map(_ => random.nextPrintableChar().toByte).toArray
          lazy val buf = Array.ofDim[Byte](random.nextInt(maxSize) + maxSize / 10)
          def continue() = {
            (written, maxSize, sizeExact) match {
              case (w, m, true) if (w < m) => m - w
              case (w, m, false) if (random.nextFloat() < chance) => chance = chance / 1.5; buf.length
              case _ => 0
            }
          }
          var writeBytes = continue()
          while (writeBytes > 0) {
            if (isTextFile) {
                val write = Math.min(writeBytes, pattern.length)
                fos.write(pattern, 0, write)
                written += pattern.length
            } else {
              val write = Math.min(writeBytes, buf.length)
              random.nextBytes(buf)
              fos.write(buf, 0, write)
              written += buf.length
            }
            writeBytes = continue()
          }
        }
        fos.close()
        l.debug("New file created "+name+" "+Utils.readableFileSize(name.length))
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
    l.debug("folder was created "+name)
    folderList += name
  }

}
