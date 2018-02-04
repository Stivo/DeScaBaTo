package ch.descabato.core.util

import java.io.File
import java.nio.file.Files
import java.text.SimpleDateFormat
import java.util.Date
import java.util.stream.Collectors

import ch.descabato.core.config.BackupFolderConfiguration

import scala.collection.JavaConverters._
import scala.util.Try

object Constants {
  val tempPrefix = "temp."
  val filesEntry = "files.txt"
  val indexSuffix = ".index"

  def objectEntry(num: Option[Int] = None): String = {
    val add = num.map(x => "_" + x).getOrElse("")
    s"content$add.obj"
  }
}

trait FileType {
  def getFiles(): Seq[File]

  def matches(file: File): Boolean

  def isMetadata(): Boolean = true

  def nextFile(): File
}

trait NumberedFileType extends FileType {
  def numberOfFile(file: File): Int

  def fileForNumber(number: Int): File
}

trait DatedFileType extends FileType {
  def dateOfFile(file: File): Date

  def getDates(): Seq[Date] = {
    getFiles().map(dateOfFile).sorted
  }

}

class FileManager(config: BackupFolderConfiguration) {
  val volume = new StandardNumberedFileType("volume", "json.gz", config)
  val volumeIndex = new StandardNumberedFileType("volumeIndex", "json.gz", config)
  val metadata = new StandardNumberedFileType("metadata", "json.gz", config)
  val backup = new StandardDatedFileType("backup", "json.gz", config)

  private val filetypes = Seq(volume, volumeIndex, metadata, backup)

  def allFiles(): Seq[File] = {
    filetypes.flatMap(_.getFiles())
  }

  def fileTypeForFile(file: File): Option[FileType] = {
    filetypes.find(_.matches(file))
  }
}

/**
  * Pattern here is:
  * $name/$name_$number for all < 1000
  * $name/$name_$range/$name_$number for all >= 1000
  */
class StandardNumberedFileType(name: String, suffix: String, config: BackupFolderConfiguration) extends NumberedFileType {
  val mainFolder = new File(config.folder, name)
  val regex = s"${name}_[0-9]+"
  val regexWithSuffix = s"${regex}\\.${suffix}"

  val filesPerFolder = 1000

  override def numberOfFile(file: File): Int = {
    require(matches(file))
    file.getName.drop(name.length + 1).takeWhile(_.isDigit).toInt
  }

  override def fileForNumber(number: Int): File = {
    val subfolderNumber = number / filesPerFolder
    val nameNumber = f"${number}%06d"
    val firstFolder = if (subfolderNumber > 0) s"${name}_$subfolderNumber/" else ""
    new File(mainFolder, s"$firstFolder${name}_${nameNumber}.${suffix}")
  }

  override def getFiles(): Seq[File] = {
    if (mainFolder.exists()) {
      val files = Files.walk(mainFolder.toPath).collect(Collectors.toList())
      files.asScala.map(_.toFile).filter(matches)
    } else {
      Seq.empty
    }
  }

  override def matches(file: File): Boolean = {
    val nameMatches = file.getName.matches(regexWithSuffix)
    val scheme1 = {
      val parentMatches = file.getParentFile.getName.matches(regex)
      val parentsParentMatches = file.getParentFile.getParentFile.getName == name
      nameMatches && parentMatches && parentsParentMatches
    }
    val scheme2 = {
      val parentMatches = file.getParentFile.getName == name
      nameMatches && parentMatches
    }
    scheme1 || scheme2
  }

  override def nextFile(): File = {
    if (mainFolder.exists()) {
      val existing = getFiles().map(numberOfFile)
      if (existing.nonEmpty) {
        fileForNumber(existing.max + 1)
      } else {
        fileForNumber(0)
      }
    } else {
      fileForNumber(0)
    }
  }
}

/**
  * Pattern here is
  * $name_$date.$suffix
  */
class StandardDatedFileType(name: String, suffix: String, config: BackupFolderConfiguration) extends DatedFileType {

  private val dateFormat = "yyyy-MM-dd.HHmmss"
  private val dateFormatter = new SimpleDateFormat(dateFormat)

  def newestFile(): Option[File] = {
    getFiles().sortBy(_.getName).lastOption
  }

  def forDate(d: Date): File = {
    val files = getFiles().filter(x => dateOfFile(x) == d)
    require(files.size == 1)
    files.head
  }

  override def dateOfFile(file: File): Date = {
    require(matches(file))
    val date = file.getName.drop(name.length + 1).take(dateFormat.length)
    dateFormatter.parse(date)
  }

  override def getFiles(): Seq[File] = {
    config.folder.listFiles().filter(_.isFile).filter(matches)
  }

  override def matches(file: File): Boolean = {
    val fileName = file.getName
    if (fileName.startsWith(name + "_") && fileName.endsWith("." + suffix)) {
      val dateString = fileName.drop(name.length + 1).dropRight(suffix.length + 1)
      Try(dateFormatter.parse(dateString)).isSuccess
    } else {
      false
    }
  }

  override def nextFile(): File = {
    val date = new Date()
    new File(config.folder, s"${name}_${dateFormatter.format(date)}.$suffix")
  }
}
