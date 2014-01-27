package ch.descabato

import scala.reflect.ClassTag
import scala.collection.mutable.Buffer
import scala.collection.mutable.HashMap
import java.io.File
import java.text.SimpleDateFormat
import java.util.Date
import java.io.FileInputStream
import java.io.BufferedOutputStream
import java.io.BufferedInputStream
import com.fasterxml.jackson.core.JsonProcessingException
import net.java.truevfs.access.TFile
import net.java.truevfs.access.TFileOutputStream
import net.java.truevfs.access.TVFS
import net.java.truevfs.access.TFileInputStream
import net.java.truevfs.access.TFileWriter

class VolumeIndex extends HashMap[String, Int]

class Volume extends HashMap[String, Array[Byte]]

class Parity

object Constants {
  val tempPrefix = "temp."
  val filesEntry = "files.txt"
  val objectEntry = "content.obj"
}

/**
 * Describes the file patterns for a certain kind of file.
 * Supports a global prefix from the BackupFolderConfiguration,
 * a prefix for it's file type, a date and a temp modifier.
 * Can be used to get all files of this type from a folder and to
 * read and write objects using serialization.
 */
case class FileType[T](prefix: String, metadata: Boolean, suffix: String)(implicit val m: Manifest[T]) extends Utils {
  import Constants._

  def globalPrefix = config.prefix

  var config: BackupFolderConfiguration = null
  var fileManager: FileManager = null

  var local = true
  var remote = true
  var redundant = false
  var hasDate = false

  def this(prefix: String, metadata: Boolean, suffix: String,
    localC: Boolean = true, remoteC: Boolean = true, redundantC: Boolean = false, hasDateC: Boolean = false)(implicit m: Manifest[T]) {
    this(prefix, metadata, suffix)
    local = localC
    remote = remoteC
    redundant = redundantC
    hasDate = hasDateC
  }

  def nextNum(temp: Boolean = false) = {
    val col = (if (temp) getTempFiles() else getFiles()).map(getNum)
    if (col.isEmpty) {
      0
    } else {
      col.max + 1
    }
  }

  def nextName(tempFile: Boolean = false) = {
    val temp = if (tempFile) tempPrefix else ""
    val date = if (hasDate) fileManager.dateFormat.format(fileManager.startDate) + "_" else ""
    val add = if (m.runtimeClass == classOf[Parity]) "" else s"${config.raes}"
    s"$globalPrefix$temp$prefix$date${nextNum(tempFile)}$suffix$add"
  }

  def nextFile(f: File = config.folder, temp: Boolean = false) = new File(f, nextName(temp))

  def matches(x: File) = x.getName().startsWith(globalPrefix + prefix)

  def getFiles(f: File = config.folder) = f.
    listFiles().filter(_.isFile()).
    filter(_.getName().startsWith(globalPrefix + prefix))

  def getTempFiles(f: File = config.folder) = f.
    listFiles().filter(_.isFile()).
    filter(_.getName().startsWith(globalPrefix + tempPrefix + prefix))

  def deleteTempFiles(f: File = config.folder) = getTempFiles(f).foreach(_.delete)

  /**
   * Removes the global prefix, the temp prefix and the file type prefix.
   * Leaves the date (if there) and number and suffix
   */
  def stripPrefixes(f: File) = {
    var rest = f.getName().drop(globalPrefix.length())
    if (rest.startsWith(tempPrefix)) {
      rest = rest.drop(tempPrefix.length)
    }
    assert(rest.startsWith(prefix))
    rest = rest.drop(prefix.length)
    rest
  }

  def getDate(x: File) = {
    val name = stripPrefixes(x)
    val date = name.take(fileManager.dateFormatLength)
    fileManager.dateFormat.parse(date)
  }

  def getNum(x: File) = {
    var rest = stripPrefixes(x)
    if (hasDate)
      rest = rest.drop(fileManager.dateFormatLength + 1)
    val num = rest.takeWhile(x => (x + "").matches("\\d"))
    if (num.isEmpty()) {
      -1
    } else
      num.toInt
  }

  def write(x: T, temp: Boolean = false) = {
    val file = nextFile(temp = temp)
    val w = new ZipFileWriter(file)
    try {
      w.enableCompression
      val desc = new ZipEntryDescription(objectEntry, config.serializerType, m.toString)
      w.writeManifest(config)
      w.writeJson(filesEntry, List(desc))
      w.writeEntry(objectEntry) { fos =>
        config.serialization().writeObject(x, fos)
      }
    } finally {
      w.close
    }
    file
  }

  def read(f: File, first: Boolean = true): Option[T] = {
    var firstCopy = first
    val reader = new ZipFileReader(f)
    try {
      Par2Handler.getHashIfCovered(f).foreach { hash =>
        reader.verifyMd5(hash)
        // Do not try to repair again
        firstCopy = false
      }
      val entries = reader.getJson[List[ZipEntryDescription]](filesEntry)
      entries match {
        case Left(ZipEntryDescription(name, serType, clas) :: Nil) => {
          val in = reader.getStream(name)
          val either = config.serialization(serType).readObject[T](in)
          in.close
          either match {
            case Left(read) => return Some(read)
            case Right(e) => throw new BackupCorruptedException(f).initCause(e)
          }
        }
        case Right(e) => throw new BackupCorruptedException(f).initCause(e)
        case _ => throw new BackupCorruptedException(f)
      }
    } catch {
      case c: SecurityException => throw c
      case e: Exception if ((e.getMessage + e.getStackTraceString).contains("CipherInputStream")) => {
        throw new PasswordWrongException("Exception while loading " + f + ", most likely the supplied passphrase is wrong.", e)
      }
      case e @ BackupCorruptedException(f, false) if firstCopy => {
        logException(e)
        reader.close
        Par2Handler.tryRepair(f, config)
        // tryRepair will throw exception if repair fails, so if we end up here, just try again
        return read(f, false)
      }
      case e: BackupCorruptedException => {
        throw e
      }
      case e: Exception =>
        l.warn("Exception while loading " + f + ", file may be corrupt")
        logException(e)
        None
    } finally {
      reader.close
    }
  }

}

/**
 * Provides different file types and does some common backup file operations.
 */
class FileManager(config: BackupFolderConfiguration) {
  val dateFormat = new SimpleDateFormat("yyyy-MM-dd.HHmmss.SSS")
  val dateFormatLength = dateFormat.format(new Date()).length()
  val startDate = new Date()

  def getDateFormatted = dateFormat.format(startDate)

  val volumes = new FileType[Volume]("volume_", false, ".zip", localC = false)
  val hashchains = new FileType[Buffer[(BAWrapper2, Array[Byte])]]("hashchains_", false, ".zip")
  val files = new FileType[Buffer[BackupPart]]("files_", true, ".zip", hasDateC = true)
  val filesDelta = new FileType[Buffer[UpdatePart]]("filesdelta_", true, ".zip", hasDateC = true)
  val index = new FileType[VolumeIndex]("index_", true, ".zip", redundantC = true)
  val par2File = new FileType[Parity]("par_", true, ".par2", localC = false, redundantC = true)
  val par2ForVolumes = new FileType[Parity]("par_volume_", true, ".par2", localC = false, redundantC = true)
  val par2ForHashChains = new FileType[Parity]("par_hashchain_", true, ".par2", localC = false, redundantC = true)
  val par2ForFiles = new FileType[Parity]("par_files_", true, ".par2", localC = false, redundantC = true)
  val par2ForFilesDelta = new FileType[Parity]("par_filesdelta_", true, ".par2", localC = false, redundantC = true)

  private val types = List(volumes, hashchains, files, filesDelta, index, par2ForFiles, par2ForVolumes, par2ForHashChains, par2ForFilesDelta, par2File)

  types.foreach { x => x.config = config; x.fileManager = this }

  def getFileType(x: File) = types.find(_.matches(x)).get

  def getBackupAndUpdates(temp: Boolean = true): (Array[File], Boolean) = {
    val filesNow = config.folder.listFiles().filter(_.isFile()).filter(files.matches)
    val sorted = filesNow.sortBy(_.getName())
    val lastDate = if (sorted.isEmpty) new Date(0) else files.getDate(sorted.filterNot(_.getName.startsWith(Constants.tempPrefix)).last)
    val onlyLast = sorted.dropWhile(x => files.getDate(x).before(lastDate))
    val updates = filesDelta.getFiles().dropWhile(filesDelta.getDate(_).before(lastDate))
    val out1 = onlyLast ++ updates
    val complete = if (temp) out1 ++ files.getTempFiles() else out1
    (complete, !updates.isEmpty)
  }

  def getBackupDates(): Seq[Date] = {
    files.getFiles().map(files.getDate).toList.distinct.sorted
  }

  def getBackupForDate(d: Date): Seq[File] = {
    files.getFiles().filter(f => files.getDate(f) == d)
  }

}