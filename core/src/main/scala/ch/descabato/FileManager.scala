package ch.descabato

import scala.reflect.ClassTag
import scala.collection.mutable.Buffer
import scala.collection.mutable.HashMap
import java.io.File
import java.text.SimpleDateFormat
import java.util.Date
import java.io.FileOutputStream
import java.io.FileInputStream

class VolumeIndex extends HashMap[String, Int]

class Volume extends HashMap[String, Array[Byte]]

class Parity

object Constants {
  val tempPrefix = "temp."
}

case class FileType[T](prefix: String, metadata: Boolean, suffix: String)(implicit val m: Manifest[T]) extends Utils {
  import Constants.tempPrefix

  var options: BackupFolderConfiguration = null
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
    s"$temp$prefix$date${nextNum(tempFile)}$suffix"
  }

  def nextFile(f: File = options.folder, temp: Boolean = false) = new File(f, nextName(temp))

  def matches(x: File) = x.getName().startsWith(prefix)

  def getFiles(f: File = options.folder) = f.
    listFiles().filter(_.isFile()).
    filter(_.getName().startsWith(prefix))

  def getTempFiles(f: File = options.folder) = f.
    listFiles().filter(_.isFile()).
    filter(_.getName().startsWith(tempPrefix + prefix))

  def deleteTempFiles(f: File = options.folder) = getTempFiles(f).foreach(_.delete)

  def getDate(x: File) = {
    val name = {
      val name = x.getName()
      if (name.startsWith(tempPrefix)) name.drop(tempPrefix.length) else name
    }
    val date = name.drop(prefix.length()).take(fileManager.dateFormatLength)
    fileManager.dateFormat.parse(date)
  }

  def getNum(x: File) = {
    val name = {
      val name = x.getName()
      if (name.startsWith(tempPrefix)) name.drop(tempPrefix.length) else name
    }
    var dropLength = prefix.length
    if (hasDate)
      dropLength += fileManager.dateFormatLength + 1
    val num = name.drop(dropLength).takeWhile(x => (x + "").matches("\\d"))
    if (num.isEmpty()) {
      -1
    } else
      num.toInt
  }

  def write(x: T, temp: Boolean = false) = {
    val file = nextFile(temp = temp)
    //l.info("Writing "+x+" to file "+file)
    val out = StreamHeaders.wrapStream(new FileOutputStream(file), options)
    options.serialization.writeObject(x, out)
    file
  }

  def read(f: File): Option[T] = {
    //l.info("Writing "+x+" to file "+file)
    //    options.serialization.readObject(x, file)(options, m)
    //    file
    try {
      val read = StreamHeaders.readStream(new FileInputStream(f), options.passphrase)
      options.serialization.readObject[T](read)
    } catch {
      case e: Exception =>
        l.warn("Exception while loading " + f)
        logException(e)
        None
    }
  }

}

class FileManager(options: BackupFolderConfiguration) {
  val dateFormat = new SimpleDateFormat("yyyy-MM-dd.HHmmss.SSS")
  val dateFormatLength = dateFormat.format(new Date()).length()
  val startDate = new Date()

  val volumes = new FileType[Volume]("volume_", false, ".zip", localC = false)
  val hashchains = new FileType[Buffer[(BAWrapper2, Array[Byte])]]("hashchains_", false, ".obj")
  val files = new FileType[Buffer[BackupPart]]("files_", true, ".obj", hasDateC = true)
  val filesDelta = new FileType[Buffer[UpdatePart]]("filesdelta_", true, ".obj", hasDateC = true)
  val index = new FileType[VolumeIndex]("index_", true, ".obj", redundantC = true)
  val par2File = new FileType[Parity]("par_", true, ".par2", localC = false, redundantC = true)
  val par2ForVolumes = new FileType[Parity]("par_volume_", true, ".par2", localC = false, redundantC = true)
  val par2ForHashChains = new FileType[Parity]("par_hashchain_", true, ".par2", localC = false, redundantC = true)
  val par2ForFiles = new FileType[Parity]("par_files_", true, ".par2", localC = false, redundantC = true)
  val par2ForFilesDelta = new FileType[Parity]("par_filesdelta_", true, ".par2", localC = false, redundantC = true)

  private val types = List(volumes, hashchains, files, filesDelta, index, par2ForFiles, par2ForVolumes, par2ForHashChains, par2ForFilesDelta, par2File)

  types.foreach { x => x.options = options; x.fileManager = this }

  def getFileType(x: File) = types.find(_.matches(x)).get

  def getBackupAndUpdates(temp: Boolean = true): (Array[File], Boolean) = {
    val filesNow = options.folder.listFiles().filter(_.isFile()).filter(files.matches)
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

  def getBackupForDate(d: Date) : Seq[File] = {
    files.getFiles().filter(f => files.getDate(f) == d)
  }
  
}