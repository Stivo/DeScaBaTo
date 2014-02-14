package ch.descabato.utils

import java.io.File
import java.util.zip.{ ZipFile => JZipFile }
import java.util.zip.ZipEntry
import java.io.BufferedInputStream
import java.util.zip.ZipOutputStream
import java.io.FileInputStream
import java.io.OutputStream
import ch.descabato.utils.Streams.{DelegatingInputStream, CountingOutputStream, DelegatingOutputStream}
import java.io.InputStream
import net.java.truevfs.access.TFile
import net.java.truevfs.access.TFileOutputStream
import net.java.truevfs.access.TVFS
import net.java.truevfs.access.TFileInputStream
import java.io.BufferedOutputStream
import java.security.MessageDigest
import net.java.truevfs.access.TConfig
import net.java.truevfs.kernel.spec.FsAccessOption
import java.io.FileOutputStream
import ch.descabato.core.{FileType, BackupFolderConfiguration, FileManager}
import ch.descabato.version.BuildInfo
import java.nio.ByteBuffer
import java.util.zip.CRC32
import ch.descabato.utils.Implicits._
import java.util.zip.Deflater
import scala.collection.mutable

case class MetaInfo(date: String, writingVersion: String)

object ZipFileHandlerFactory {

  private var readerCache = Map[File, ZipFileReaderTFile]()

  def writer(file: File, config: BackupFolderConfiguration): ZipFileWriter = {
    if (config.hasPassword)
      new ZipFileWriterTFile(file)
    else
      new ZipFileWriterJdk(file)
  }

  def complexWriter(file: File): ComplexZipFileWriter = new ZipFileWriterTFile(file)

  def reader(file: File, config: BackupFolderConfiguration): ZipFileReader = {
    if ((readerCache safeContains file) && readerCache(file).isValid)
      readerCache(file)
    val out = new ZipFileReaderTFile(file)
    readerCache += file -> out
    out
  }
  
  def createZipEntry(name: String, header: Byte, content: ByteBuffer) = {
    val ze = new ZipEntry(name)
    ze.setMethod(ZipEntry.STORED)
    ze.setSize(content.remaining()+1)
    ze.setCompressedSize(content.remaining()+1)
    val crc = new CRC32()
    crc.update(header)
    crc.update(content.array(), content.position(), content.remaining())
    ze.setCrc(crc.getValue())
    ze
  }

}

trait ZipFileHandlerCommon {
  def close()
  val file: File
}

trait ZipFileReader extends ZipFileHandlerCommon {

  def names: Seq[String]

  def getStream(name: String): InputStream

  def getEntrySize(name: String): Long

  def getJson[T](name: String)(implicit m: Manifest[T]): Either[T, Exception] = {
    try {
      val in = getStream(name)
      try {
        val js = new JsonSerialization()
        js.readObject(in)
      } finally {
        in.close()
      }
    } catch {
      case e: Exception => Right(e)
    }
  }

  def verifyMd5(hash: Array[Byte]) = {
    val in = new FileInputStream(file)
    val read = new Streams.VerifyInputStream(in, MessageDigest.getInstance("MD5"), hash, file)
    Streams.readFrom(read, (_, _) => Unit)
    // stream is closed in readFrom
  }

}

trait ComplexZipFileWriter extends ZipFileWriter {
  def writeIntoFrom(zipFile: File, relativePath: String = ""): Boolean
  def copyFrom(file: File, path: String, name: String): Boolean
}


trait ZipFileWriter extends ZipFileHandlerCommon {

  def writeEntry(name: String)(f: (OutputStream => Unit)) {
    val bos = newOutputStream(name)
    try {
      f(bos)
    } finally {
      bos.close()
    }
  }

  def writeUncompressedEntry(zipEntry: ZipEntry, byte: Byte, content: ByteBuffer)
  
  def newOutputStream(name: String): OutputStream

  def size(): Long

  def writeJson[T](name: String, t: T)(implicit m: Manifest[T]) {
    writeEntry(name) { o =>
      val js = new JsonSerialization()
      js.writeObject(t, o)
    }
  }

  def writeManifest(fm: FileManager) {
    val versionNumber: String = BuildInfo.version
    val m = new MetaInfo(fm.getDateFormatted, versionNumber)
    writeJson("manifest.txt", m)
  }

  def enableCompression()
  
  def close()
}

abstract class ZipFileHandler(zip: File) {

  protected var _mounted = false
  protected var _configSet = false

  // All operations to files within the archive must access this variable
  protected lazy val tfile = {
    _mounted = true
    new TFile(zip);
  }

  def close() {
    if (_configSet)
      config.close()
    if (_mounted)
      TVFS.umount(tfile)
  }

  lazy val config = {
    _configSet = true
    TConfig.open()
  }

}

/**
 * A thin wrapper around a java zip file reader.
 * Needs to be closed in the end.
 */
private[this] class ZipFileReaderTFile(val file: File) extends ZipFileHandler(file) with ZipFileReader with Utils {

  private var openStreams = 0
  private var closeRequested = false

  def this(s: String) = this(new File(s))

  def names = {
    var out = mutable.Buffer[String]()
    def addAll(x: TFile, prefix: String = "") {
      val (files, folders) = x.listFiles().partition(_.isFile)
      out ++= files.map(x => prefix + x.getName)
      folders.foreach(x => addAll(x, prefix+x.getName+"/"))
    }
    addAll(tfile)
    out
  }

  def getStream(name: String): InputStream = {
    if (closeRequested)
      throw new IllegalStateException("Tried to close this zip file")
    val e = new TFile(tfile, name);
    new CountingInputStream(new BufferedInputStream(new TFileInputStream(e)))
  }

  def getEntrySize(name: String) = new TFile(tfile, name).length()

  class CountingInputStream(in: InputStream) extends DelegatingInputStream(in) {
    override def close() {
      super.close()
      this.synchronized{
        openStreams -= 1
        if (closeRequested)
          close()
      }
    }
  }

  def isValid = !closeRequested

  override def close() {
    this.synchronized {
      closeRequested = true
      if (openStreams == 0) {
        super.close()
      }
    }
  }
}

/**
 * A Zip File Writer using TrueVFS.
 * The size is a lower bound
 */
private[this] class ZipFileWriterTFile(val file: File) extends ZipFileHandler(file) with ComplexZipFileWriter {

  private var counter = 0L

  override def writeEntry(name: String)(f: (OutputStream => Unit)) {
    val bos = newOutputStream(name)
    counter += name.length() * 2
    try {
      f(bos)
    } finally {
      bos.close()
      bos match {
        case x: CountingOutputStream => counter += x.counter
        case _ =>
      }
    }
  }

  def writeUncompressedEntry(zipEntry: ZipEntry, byte: Byte, content: ByteBuffer) {
    writeEntry(zipEntry.getName) { out =>
      out.write(byte)
      content.writeTo(out)
    }
  }
  
  def newOutputStream(name: String): OutputStream = {
    val out = new TFileOutputStream(new TFile(tfile, name))
    val bos = new BufferedOutputStream(out, 20 * 1024)
    new CountingOutputStream(bos)
  }

  def size() = {
    counter
  }
  
  def enableCompression() {
    config.setAccessPreference(FsAccessOption.STORE, false)
    config.setAccessPreference(FsAccessOption.COMPRESS, true)
  }

  def writeIntoFrom(zipFile: File, relativePath: String = "") = {
    val root = new TFile(zipFile)
    val srcdir = if (relativePath != "") new TFile(root, relativePath) else root
    val dstdir = if (relativePath != "") new TFile(tfile, relativePath) else tfile
    srcdir.cp_rp(dstdir)
    TVFS.umount(root)
    true
  }

  def copyFrom(file: File, path: String, name: String) = {
    val root = new TFile(file)
    val src = new TFile(root, path)
    val dst = new TFile(tfile, name)
    src.cp(dst)
    TVFS.umount(root)
    true
  }

}

/**
 * A Zip File writer using the standard JDK classes.
 */
private[this] class ZipFileWriterJdk(val file: File) extends ZipFileHandler(file) with ZipFileWriter {

  val fos = new CountingOutputStream(new FileOutputStream(file))
  val out = new ZipOutputStream(fos)

  override def close() {
    out.finish()
    out.close()
  }
  
  def newOutputStream(name: String): OutputStream = {
    val ze = new ZipEntry(name)
    out.putNextEntry(ze)
    new DelegatingOutputStream(out) {
       override def close() = {
         out.closeEntry()
       }
    }
  }
  
  def writeUncompressedEntry(zipEntry: ZipEntry, byte: Byte, content: ByteBuffer) {
    out.putNextEntry(zipEntry)
    out.write(byte)
    content.writeTo(out)
    out.closeEntry()
  }
  
  def enableCompression() {
    out.setLevel(Deflater.BEST_COMPRESSION)
  }

  def size() = {
    fos.count
  }

}