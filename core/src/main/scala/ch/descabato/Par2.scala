package ch.descabato

import java.io.InputStream
import java.io.File
import java.io.FileInputStream
import java.nio.charset.Charset
import java.util.Arrays
import java.io.IOException
import scala.collection.mutable
import java.io.RandomAccessFile
import java.nio.channels.FileChannel
import java.nio.ByteOrder
import scala.collection.mutable.Buffer
import java.util.regex.Pattern
import scala.collection.mutable.WeakHashMap
import java.security.MessageDigest

object CommandLineToolSearcher {
  private var cache = Map[String, String]()

  private def find(name: String): String = {
    try {
      val proc = Runtime.getRuntime().exec(name)
      println(name)
      proc.waitFor() match {
        case 3 | 0 => return name
        case _ =>
      }
    } catch {
      case _: Exception => // ignore
    }
    val candidates = List(s"tools/$name", name, s"../tools/$name", s"../../tools/$name")
    candidates.map(new File(_)).find(_.exists).map(_.toString()).get
  }

  def lookUpName(name: String): String = {
    val exeName = if (Utils.isWindows) name + ".exe" else name
    if (cache contains exeName) {
      return cache(exeName)
    } else {
      val out = find(exeName)
      cache += exeName -> out
      out
    }
  }
}

/**
 * Creates par2 files for the backup folder that cover the
 * index, the hash chains and the volumes.
 */
class RedundancyHandler(config: BackupFolderConfiguration) extends Utils {

  import Par2Handler._

  lazy val fileManager = config.fileManager

  def createFiles() {
    forHashChainsAndIndex()
    forVolumes()
  }

  var numberOfFiles = 1

  def notCovered[T](ft: FileType[T]) = {
    val covered = readCoveredFiles(config.folder)
    val out = ft.getFiles()
      .filter(x => !(covered contains x))
    out
  }

  def forVolumes() {
    val p2volume = config.fileManager.par2ForVolumes
    var volumes = filesMatchingPrefix(fileManager.volumes)
    while (!volumes.isEmpty) {
      val f = p2volume.nextFile()
      start(f, volumes.take(1), 5, config.blockSize.bytes)
      volumes = volumes.drop(1)
    }
  }

  def filesMatchingPrefix[T](ft: FileType[T], sort: Boolean = true) = {
    val out = notCovered(ft)
    if (sort) {
      out.sortBy(ft.getNum(_))
    } else {
      out
    }
  }

  def handleFiles[T](parFt: FileType[Parity], ft: FileType[T], redundancy: Int) = {
    val par2File = parFt.nextFile()
    start(par2File, filesMatchingPrefix(ft, true), redundancy)
  }

  def forHashChainsAndIndex() {
    handleFiles(fileManager.par2ForFiles, fileManager.files, 50)
    handleFiles(fileManager.par2ForHashChains, fileManager.hashchains, 50)
    handleFiles(fileManager.par2ForFilesDelta, fileManager.filesDelta, 50)
  }

  /**
   * Starts the command line utility to create the par2 files
   */
  def start(par2File: File, files: Iterable[File], redundancy: Int, size: Long = 100000) {
    //import redundancy._
    if (files.isEmpty) {
      return
    }
    val par2Executable = CommandLineToolSearcher.lookUpName("par2")
    l.info(s"Starting par2 creation for ${files.size} files ${new Size(files.map(_.length()).sum)})")
    //    l.info("This may take a while")
    val cmd = Buffer[String]()
    cmd += par2Executable
    cmd += "create"
    cmd += s"-r$redundancy"
    //cmd += s"-t-"
    cmd += s"-n$numberOfFiles"
    cmd += s"-s${size}"
    cmd += par2File.getName()
    if (Utils.isWindows)
      cmd += "--"
    cmd ++= files.map(_.getName())
    if (!startProcess(cmd)) {
      l.info("par2 creation failed for " + files)
    }
  }

  def startProcess(cmd: Iterable[String]) = {
    l.info("Starting command " + cmd.mkString(" "))
    val proc = new ProcessBuilder().command(cmd.toList: _*)
      .redirectError(new File(config.folder, "par2log.txt"))
      .directory(config.folder)
      .start
    val exit = proc.waitFor()
    proc.destroy()
    exit == 0
  }

  def startRepair(par2File: File) = {
    val par2Executable = CommandLineToolSearcher.lookUpName("par2")
    val cmd = Buffer[String]()
    cmd += par2Executable
    cmd += "repair"
    cmd += par2File.getName()
    startProcess(cmd)
  }

  def repair(file: File): Boolean = {
    parseAllPar2(config.folder).find(_.coveredFiles.map(_._1).contains(file.getName())).map(_.par2File(config.folder)) match {
      case Some(f) =>
        val out = startRepair(f)
        if (out) {
          val all = file.getParentFile.listFiles().filter(_.getName().startsWith(file.getName()))
          all.sortBy(_.getName().length()).drop(1).foreach { f =>
            l.debug("Going to delete " + f)
            f.delete
          }
        }
        out
      case None => false
    }
  }

}

object Par2Handler {
  // This map contains the parsed par2files for a given par2 file
  private val cache = new mutable.HashMap[File, Par2File]() 

  def get(x: File) = {
	  if (!cache.contains(x)) {
	    cache(x) = new Par2Parser(x).parse()
	  }
	  cache(x)
  }
  
  val regex = """.*?vol\d+\+\d+\.par2""".r

  def parseAllPar2(folder: File): Seq[Par2File] = {
    val list = folder.listFiles().filter(_.getName().endsWith(".par2"))
      .filterNot(f => regex.pattern.matcher(f.getName).matches())
    list.map(Par2Handler.get(_))
  }

  // This map contains for each covered file in which par2 file it is covered
  def readCoveredFiles(folder: File): Map[File, Par2File] = {
    parseAllPar2(folder).flatMap {
      case p @ Par2File(name, list) => list.map(f => new File(folder, f._1)).map(x => (x, p))
    }.toMap
  }

  def wrapVerifyStreamIfCovered(f: File, is: InputStream) = {
    val covered = readCoveredFiles(f.getParentFile())
    covered.get(f) match {
      case Some(p @ Par2File(name, list)) =>
        new Streams.VerifyInputStream(is, MessageDigest.getInstance("MD5"), p.getHash(f.getName()).get, f)
      case None => is
    }
  }

}

/**
 * Parses a given par2 file to find out which
 * files it covers.
 */
private class Par2Parser(val f: File) {
  lazy val raf = new RandomAccessFile(f, "r")
  import Utils._

  private var files = mutable.LinkedHashMap[String, String]()
  private var hashes = mutable.Map[String, Array[Byte]]()

  private val ascii = Charset.forName("ASCII")
  private val utf = Charset.forName("UTF-16LE")
  private val magic = "PAR2\0PKT".getBytes(ascii).toArray
  private val fileDescHeader = "PAR 2.0\0FileDesc".getBytes(ascii)
  private val fileDescHeaderUnicode = "PAR 2.0\0UniFileN".getBytes(ascii)

  private def readBytes(bytes: Int) = {
    val out = Array.ofDim[Byte](bytes)
    raf.readFully(out)
    out
  }

  private def readLong() = {
    val map = raf.getChannel().map(FileChannel.MapMode.READ_ONLY, raf.getFilePointer(), 8);
    map.order(ByteOrder.LITTLE_ENDIAN);
    map.asLongBuffer().get()
  }

  class ParsingException(m: String) extends IOException(m + " at pos " + raf.getFilePointer())

  private def arrayEq(a: Array[Byte], b: Array[Byte]) = Arrays.equals(a, b)

  private def parseOnePacket() {
    var start = raf.getFilePointer()
    if (!arrayEq(readBytes(8), magic)) {
      throw new ParsingException("Expected header ")
    }
    val length = readLong()
    raf.skipBytes(40)
    val typ = readBytes(16)
    if (arrayEq(typ, fileDescHeader)) {
      val id = readBytes(16)
      val hash = readBytes(16)
      raf.skipBytes(24)
      val stringLength = length + start - raf.getFilePointer()
      val name = new String(readBytes(stringLength.toInt), ascii).trim
      files(encodeBase64(id)) = name
      hashes(encodeBase64(id)) = hash
      if (raf.getFilePointer() != length + start) {
        throw new ParsingException("Not at end of packet somehow")
      }
    } else if (arrayEq(typ, fileDescHeaderUnicode)) {
      val id = readBytes(16)
      val stringLength = length + start - raf.getFilePointer()
      val name = new String(readBytes(stringLength.toInt), utf).trim
      files(encodeBase64(id)) = name
      if (raf.getFilePointer() != length + start) {
        throw new ParsingException("Not at end of packet somehow")
      }
    } else {
      raf.seek(length + start)
    }
  }

  def parse(): Par2File = {
    try {
      while (raf.length() > raf.getFilePointer())
        parseOnePacket
    } finally {
      raf.close()
    }
    Par2File(f.getName(), files.map(_._1).map(x => (files(x), hashes(x))).toSeq)
  }
}

case class Par2File(name: String, coveredFiles: Seq[(String, Array[Byte])]) {
  def getHash(filename: String) = coveredFiles.find(_._1 == filename).map(_._2)
  def par2File(folder: File) = new File(folder, name)
}