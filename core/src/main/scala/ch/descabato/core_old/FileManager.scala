package ch.descabato.core_old

import java.io.File
import java.nio.file.Files
import java.text.SimpleDateFormat
import java.util.Date
import java.util.stream.Collectors

import ch.descabato.core.actors.MetadataStorageActor.{BackupDescription, BackupMetaDataStored}
import ch.descabato.core.model.StoredChunk
import ch.descabato.utils.{Hash, Utils}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Try

class VolumeIndex extends mutable.HashMap[String, Int]

class Volume extends mutable.HashMap[String, Array[Byte]]

class Parity

object Constants {
  val tempPrefix = "temp."
  val filesEntry = "files.txt"
  val indexSuffix = ".index"

  def objectEntry(num: Option[Int] = None): String = {
    val add = num.map(x => "_" + x).getOrElse("")
    s"content$add.obj"
  }
}

trait FileTypeNew {
  def getFiles(): Seq[File]

  def matches(file: File): Boolean

  def isMetadata(): Boolean = true

  def nextFile(): File
}

trait NumberedFileTypeNew extends FileTypeNew {
  def numberOfFile(file: File): Int

  def fileForNumber(number: Int): File
}

trait DatedFileTypeNew extends FileTypeNew {
  def dateOfFile(file: File): Date

  def getDates(): Seq[Date] = {
    getFiles().map(dateOfFile).sorted
  }

}

class FileManagerNew(config: BackupFolderConfiguration) {
  val volume = new StandardNumberedFileType("volume", "json.gz", config)
  val volumeIndex = new StandardNumberedFileType("volumeIndex", "json.gz", config)
  val metadata = new StandardNumberedFileType("metadata", "json.gz", config)
  val backup = new StandardDatedFileType("backup", "json.gz", config)

  private val filetypes = Seq(volume, volumeIndex, metadata, backup)

  def allFiles(): Seq[File] = {
    filetypes.flatMap(_.getFiles())
  }

  def fileTypeForFile(file: File): Option[FileTypeNew] = {
    filetypes.find(_.matches(file))
  }
}

/**
  * Pattern here is: $name/$name_$range/$name_$number
  */
class StandardNumberedFileType(name: String, suffix: String, config: BackupFolderConfiguration) extends NumberedFileTypeNew {
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
    new File(mainFolder, s"${name}_$subfolderNumber/${name}_${nameNumber}.${suffix}")
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
    val parentMatches = file.getParentFile.getName.matches(regex)
    val parentsParentMatches = file.getParentFile.getParentFile.getName == name
    nameMatches && parentMatches && parentsParentMatches
  }

  override def nextFile(): File = {
    if (mainFolder.exists()) {
      val subdirs = mainFolder.listFiles().filter(_.isDirectory).filter(x => x.getName.matches(regex)).sorted
      if (subdirs.nonEmpty) {
        val files = subdirs.last.listFiles().filter(matches).sorted
        if (files.nonEmpty) {
          val number = numberOfFile(files.last)
          return fileForNumber(number + 1)
        }
      }
    }
    fileForNumber(0)
  }
}

/**
  * Pattern here is
  * $name_$date.$suffix
  */
class StandardDatedFileType(name: String, suffix: String, config: BackupFolderConfiguration) extends DatedFileTypeNew {

  private val dateFormat = "yyyy-MM-dd.HHmmss"
  private val dateFormatter = new SimpleDateFormat(dateFormat)

  def newestFile(): File = {
    getFiles().sortBy(_.getName).last
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

/**
  * Describes the file patterns for a certain kind of file.
  * Supports a global prefix from the BackupFolderConfiguration,
  * a prefix for it's file type, a date and a temp modifier.
  * Can be used to get all files of this type from a folder and to
  * read and write objects using serialization.
  */
case class FileType[T](prefix: String, metadata: Boolean, suffix: String)(implicit val m: Manifest[T]) extends Utils {

  import ch.descabato.core_old.Constants._

  def globalPrefix: String = config.prefix

  var config: BackupFolderConfiguration = _
  var fileManager: FileManager = _

  var local = true
  var remote = true
  var redundant = false
  var hasDate = false
  var useSubfolder = true

  def shouldDeleteIncomplete = false

  def this(prefix: String, metadata: Boolean, suffix: String,
           localC: Boolean = true, remoteC: Boolean = true, redundantC: Boolean = false, hasDateC: Boolean = false, useSubfolder: Boolean = true)(implicit m: Manifest[T]) {
    this(prefix, metadata, suffix)
    this.local = localC
    this.remote = remoteC
    this.redundant = redundantC
    this.hasDate = hasDateC
    this.useSubfolder = useSubfolder
  }

  lazy val subfolder: String = {
    var prefixFolder = if (useSubfolder) s"$prefix/" else ""
    if (globalPrefix != null) {
      s"$globalPrefix/$prefixFolder"
    } else {
      s"$prefixFolder"
    }
  }

  def nextNum(temp: Boolean = false): Int = {
    val col = (if (temp) getTempFiles() else getFiles()).map(numberOf)
    val fromJournal = fileManager.usedIdentifiers
    val col2 = col ++ fromJournal.map(x => new File(config.folder, x)).filter(matches).filter(isTemp(_) == temp).map(numberOf)
    if (col2.isEmpty) {
      0
    } else {
      col2.max + 1
    }
  }

  private def nextName(tempFile: Boolean = false): String = {
    filenameForNumber(nextNum(tempFile), tempFile)
  }

  def nextFile(f: File = config.folder, temp: Boolean = false) = {
    val out = new File(f, subfolder + nextName(temp))
    out.getParentFile.mkdirs()
    out
  }

  private def isInCorrectFolder(x: File): Boolean = {
    var parentsToMatch = subfolder.split("/").toSeq.filter(_.nonEmpty)
    var parent1 = x.getParentFile
    while (parentsToMatch.nonEmpty) {
      if (!parent1.getName.equals(parentsToMatch.last)) {
        return false
      }
      parentsToMatch = parentsToMatch.init
      parent1 = parent1.getParentFile
    }
    true
  }

  def matches(x: File): Boolean = {
    if (!isInCorrectFolder(x)) {
      return false
    }
    var name = x.getName()
    if (name.startsWith(tempPrefix)) {
      name = name.drop(tempPrefix.length)
    }
    if (!name.startsWith(prefix)) {
      return false
    }
    name = name.drop(prefix.length).drop(1)
    if (hasDate) {
      try {
        fileManager.dateFormat.parse(name.take(fileManager.dateFormatLength))
        // there is an underscore after the date
        name = name.drop(fileManager.dateFormatLength + 1)
      } catch {
        case e: java.text.ParseException => return false
      }
    }
    val num = name.takeWhile(_.isDigit)
    if (num.length() == 0) {
      return false
    }
    name = name.drop(num.length())
    name.equals(suffix)
  }

  def getFiles(f: File = config.folder): Seq[File] = {
    val folder = new File(f, subfolder)
    if (!folder.exists()) {
      Seq.empty
    } else {
      folder.
        listFiles().view.filter(_.isFile()).
        filter(matches).
        filterNot(isTemp).
        filterNot(_.getName.endsWith(".tmp")).force
    }
  }

  def getTempFiles(f: File = config.folder): Seq[File] = {
    val folder = new File(f, subfolder)
    if (!folder.exists()) {
      Seq.empty
    } else {
      folder.listFiles().view.filter(_.isFile()).
        filter(_.getName().startsWith(globalPrefix + tempPrefix + prefix)).
        filter(matches).
        filterNot(_.getName.endsWith(".tmp")).force
    }
  }


  def deleteTempFiles(f: File = config.folder): Unit = getTempFiles(f).foreach(_.delete)

  /**
    * Removes the global prefix, the temp prefix and the file type prefix.
    * Leaves the date (if there) and number and suffix
    */
  def stripPrefixes(f: File): String = {
    var rest = f.getName().drop(globalPrefix.length())
    if (rest.startsWith(tempPrefix)) {
      rest = rest.drop(tempPrefix.length)
    }
    assert(rest.startsWith(prefix))
    rest = rest.drop(prefix.length).drop(1)
    rest
  }

  def date(x: File): Date = {
    val name = stripPrefixes(x)
    val date = name.take(fileManager.dateFormatLength)
    fileManager.dateFormat.parse(date)
  }

  def numberOf(x: File): Int = {
    var rest = stripPrefixes(x)
    if (hasDate) {
      rest = rest.drop(fileManager.dateFormatLength + 1)
    }
    val num = rest.takeWhile(x => (x + "").matches("\\d"))
    if (num.isEmpty()) {
      -1
    } else {
      num.toInt
    }
  }

  def fileForNumber(number: Int, tempFile: Boolean = false, folder: File = config.folder): File = {
    new File(folder, subfolder + "/" + filenameForNumber(number, tempFile))
  }

  def filenameForNumber(number: Int, tempFile: Boolean = false): String = {
    val temp = if (tempFile) tempPrefix else ""
    val date = if (hasDate) fileManager.dateFormat.format(fileManager.startDate) + "_" else ""
    s"$temp${prefix}_$date$number$suffix"
  }

  def isTemp(file: File): Boolean = {
    require(matches(file))
    file.getName.startsWith(tempPrefix)
  }

}

trait Index

class IndexFileType(val filetype: FileType[_]) extends FileType[Index](filetype.prefix + "_index", true, ".kvs") {

  private def getParentFolder(file: File, fileType: FileType[_]) = {
    var parent = file.getParentFile
    filetype.subfolder.split("/").filter(_.nonEmpty).foreach { _ =>
      parent = parent.getParentFile
    }
    parent
  }

  def indexForFile(indexedFile: File): File = {
    val parent = getParentFolder(indexedFile, filetype)
    new File(parent, subfolder + filenameForNumber(filetype.numberOf(indexedFile)))
  }

  def fileForIndex(indexFile: File): File = {
    val parent = getParentFolder(indexFile, this)
    new File(indexFile.getParentFile(), filetype.subfolder + filetype.filenameForNumber(numberOf(indexFile)))
  }

  override val shouldDeleteIncomplete = true
}

/**
  * Provides different file types and does some common backup file operations.
  */
class FileManager(val usedIdentifiers: Set[String], val config: BackupFolderConfiguration) {
  if (config == null) {
    throw new NullPointerException("config must be set")
  }

  val dateFormat = new SimpleDateFormat("yyyy-MM-dd.HHmmss.SSS")
  val dateFormatLength: Int = dateFormat.format(new Date()).length()
  val startDate = new Date()

  def getDateFormatted: String = dateFormat.format(startDate)

  val backup = new FileType[BackupDescription]("backup", true, ".json", hasDateC = true, useSubfolder = false)
  val volume = new FileType[Volume]("volume", true, ".kvs", localC = false)
  val volumeIndex = new FileType[Seq[StoredChunk]]("volumeIndex", true, ".json")
  val metadata = new FileType[BackupMetaDataStored]("metadata", true, ".json")

  val volumes_old = new FileType[Volume]("volume_old", false, ".kvs", localC = false)
  val volumeIndex_old = new IndexFileType(volumes_old)
  val hashlists = new FileType[Vector[(Hash, Array[Byte])]]("hashlists", false, ".kvs")
  val backup_old = new FileType[BackupDescriptionOld]("backup_old", true, ".kvs", hasDateC = true, useSubfolder = false)
  val filesDelta = new FileType[mutable.Buffer[UpdatePart]]("filesdelta", true, ".kvs", hasDateC = true)
  //val index = new FileType[VolumeIndex]("index_", true, ".zip", redundantC = true)
  //  val par2File = new FileType[Parity]("par_", true, ".par2", localC = false, redundantC = true)
  //  val par2ForVolumes = new FileType[Parity]("par_volume_", true, ".par2", localC = false, redundantC = true)
  //  val par2ForHashLists = new FileType[Parity]("par_hashlist_", true, ".par2", localC = false, redundantC = true)
  //  val par2ForFiles = new FileType[Parity]("par_files_", true, ".par2", localC = false, redundantC = true)
  //  val par2ForFilesDelta = new FileType[Parity]("par_filesdelta_", true, ".par2", localC = false, redundantC = true)

  private val types = List(volumes_old, volumeIndex_old, hashlists, filesDelta, backup_old, metadata, volumeIndex, volume, backup)
  //, par2ForFiles, par2ForVolumes, par2ForHashLists, par2ForFilesDelta, par2File)

  def allFiles(temp: Boolean = true): List[File] = types.flatMap(ft => ft.getFiles() ++ (if (temp) ft.getTempFiles() else Nil))

  types.foreach { x => x.config = config; x.fileManager = this }

  def getFileType(x: File): FileType[_] = types.find(_.matches(x)).get

  // TODO some of this code is needed later
  //  def getBackupAndUpdates(temp: Boolean = true): (Array[File], Boolean) = {
  //    val filesNow = config.folder.listFiles().filter(_.isFile()).filter(files.matches)
  //    val sorted = filesNow.sortBy(_.getName())
  //    val lastDate = if (sorted.isEmpty) new Date(0) else files.date(sorted.filterNot(_.getName.startsWith(Constants.tempPrefix)).last)
  //    val onlyLast = sorted.dropWhile(x => files.date(x).before(lastDate))
  //    val updates = filesDelta.getFiles().dropWhile(filesDelta.date(_).before(lastDate))
  //    val out1 = onlyLast ++ updates
  //    val complete = if (temp) out1 ++ files.getTempFiles() else out1
  //    (complete, !updates.isEmpty)
  //  }

  def getLastBackupOld(temp: Boolean = false): List[File] = {
    val out = backup_old.getFiles().sortBy(f => (backup_old.date(f), backup_old.numberOf(f))).lastOption.toList
    if (temp) {
      out ++ backup_old.getTempFiles().sortBy(backup_old.numberOf)
    } else {
      out
    }
  }

  def getLastBackup(temp: Boolean = false): Seq[File] = {
    val out = backup.getFiles().sortBy(f => (backup.date(f), backup.numberOf(f))).lastOption.toList
    if (temp) {
      out ++ backup.getTempFiles().sortBy(backup.numberOf)
    } else {
      out
    }
  }

  def getBackupDatesOld(): Seq[Date] = {
    backup_old.getFiles().map(backup_old.date).toList.distinct.sorted
  }

  def getBackupDates(): Seq[Date] = {
    backup.getFiles().map(backup.date).toList.distinct.sorted
  }

  def getBackupForDateOld(d: Date): Seq[File] = {
    backup_old.getFiles().filter(f => backup_old.date(f) == d)
  }

  def getBackupForDate(d: Date): Seq[File] = {
    backup.getFiles().filter(f => backup.date(f) == d)
  }

}