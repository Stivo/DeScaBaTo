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

object CommandLineToolSearcher {
  private var cache = Map[String, String]()
  
  private def find(name: String): String = {
   try {
      val proc = Runtime.getRuntime().exec(name)
      if (proc.waitFor() == 0) {
        return name
      }
    } catch {
      case _ : Exception => // ignore
    }
    val candidates = List(s"tools/$name", name, s"../tools/$name", s"../../tools/$name") 
    candidates.map(new File(_)).find(_.exists).map(_.toString()).get
  }
  
  def lookUpName(name: String): String = {
    val exeName = if (Utils.isWindows) name+".exe" else name
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

  lazy val fileManager = config.fileManager

  def readCoveredFiles: Set[File] = {
    val list = config.folder.listFiles
      .filter(_.isFile).filter(_.getName().endsWith(".par2"))
    list.map(f => new Par2Parser(f).parse()).fold(List())(_ ++ _).toSet
  }

  def createFiles {
    forHashChainsAndIndex
    forVolumes
  }

  var numberOfFiles = 1

  def notCovered[T](ft: FileType[T]) = {
    val covered = readCoveredFiles
    val out = ft.getFiles()
      .filter(x => !(covered contains x))
    out
  }

  def forVolumes {
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

  def forHashChainsAndIndex {
    //    val backup = redundancy.percentage
    //    try {
    //      redundancy.percentage = 50
    handleFiles(fileManager.par2ForFiles, fileManager.files, 50)
    handleFiles(fileManager.par2ForHashChains, fileManager.hashchains, 50)
    handleFiles(fileManager.par2ForFilesDelta, fileManager.filesDelta, 50)
    //    } finally {
    //      redundancy.percentage = backup
    //    }
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
    l.info("This may take a while")
    val cmd = Buffer[String]()
    cmd += par2Executable
    cmd += "create"
    cmd += s"-r$redundancy"
    cmd += s"-t-"
    cmd += s"-n$numberOfFiles"
    cmd += s"-s${size}"
    cmd += par2File.getName()
    cmd += "--"
    cmd ++= files.map(_.getName())
    l.info("Starting command "+cmd.mkString(" "))
    val proc = new ProcessBuilder().command(cmd: _*)
      //.redirectError(new File(par2File.getParentFile(), "par2log.txt"))
      .directory(config.folder)
      .start
    val exit = proc.waitFor()
    if (exit == 0) {
      l.info("par2 creation ended successfully")
    } else {
      l.info("par2 creation failed")
    }
  }

}

/**
 * Parses a given par2 file to find out which
 * files it covers.
 */
class Par2Parser(val f: File) {
  val raf = new RandomAccessFile(f, "r")
  import Utils._

  var files = mutable.Map[String, String]()

  val ascii = Charset.forName("ASCII")
  val utf = Charset.forName("UTF-16LE")
  val magic = "PAR2\0PKT".getBytes(ascii).toArray
  val fileDescHeader = "PAR 2.0\0FileDesc".getBytes(ascii)
  val fileDescHeaderUnicode = "PAR 2.0\0UniFileN".getBytes(ascii)

  def readBytes(bytes: Int) = {
    val out = Array.ofDim[Byte](bytes)
    raf.readFully(out)
    out
  }

  def readLong() = {
    val map = raf.getChannel().map(FileChannel.MapMode.READ_ONLY, raf.getFilePointer(), 8);
    map.order(ByteOrder.LITTLE_ENDIAN);
    map.asLongBuffer().get()
  }

  class ParsingException(m: String) extends IOException(m + " at pos " + raf.getFilePointer())

  def arrayEq(a: Array[Byte], b: Array[Byte]) = Arrays.equals(a, b)

  def parseOnePacket {
    var start = raf.getFilePointer()
    if (!arrayEq(readBytes(8), magic)) {
      throw new ParsingException("Expected header ")
    }
    val length = readLong()
    raf.skipBytes(40)
    val typ = readBytes(16)
    if (arrayEq(typ, fileDescHeader)) {
      val id = readBytes(16)
      raf.skipBytes(40)
      val stringLength = length + start - raf.getFilePointer()
      val name = new String(readBytes(stringLength.toInt), ascii).trim
      files(encodeBase64(id)) = name
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

  def parse() = {
    while (raf.length() > raf.getFilePointer())
      parseOnePacket
    raf.close()
    files.values.map(new File(f.getParentFile(), _))
  }
}

