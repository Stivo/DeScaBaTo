package ch.descabato

import java.io.File
import scala.collection.JavaConversions._
import java.util.zip.{ ZipFile => JZipFile }
import java.util.zip.ZipEntry
import scala.collection.mutable.Buffer
import java.io.BufferedInputStream
import java.util.zip.ZipOutputStream
import java.io.FileInputStream
import java.io.OutputStream
import ch.descabato.Streams.CountingOutputStream
import java.io.InputStream
import ch.descabato.Streams.DelegatingInputStream
import net.java.truevfs.access.TFile
import net.java.truevfs.access.TFileOutputStream
import net.java.truevfs.access.TVFS
import net.java.truevfs.access.TFileInputStream
import java.io.BufferedOutputStream
import java.security.MessageDigest
import net.java.truevfs.access.TConfig
import net.java.truevfs.kernel.spec.FsAccessOption
import java.io.IOException

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

  def enableCompression() {
    config.setAccessPreference(FsAccessOption.STORE, false)
    config.setAccessPreference(FsAccessOption.COMPRESS, true)
  }

}

/**
 * A thin wrapper around a java zip file reader.
 * Needs to be closed in the end.
 */
class ZipFileReader(val file: File) extends ZipFileHandler(file) with Utils {

  def this(s: String) = this(new File(s))

  lazy val names = tfile.list().toArray

  def getStream(name: String): InputStream = {
    val e = new TFile(tfile, name);
    new BufferedInputStream(new TFileInputStream(e))
  }

  def getEntrySize(name: String) = new TFile(tfile, name).length()

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
    val in = new BufferedInputStream(new FileInputStream(file))
    val read = new Streams.VerifyInputStream(in, MessageDigest.getInstance("MD5"), hash, file)
    Streams.readFrom(read, (_, _) => Unit)
    // stream is closed in readFrom
  }

}

/**
 * A thin wrapper around a zip file writer.
 * Has the current file size.
 */
class ZipFileWriter(val file: File) extends ZipFileHandler(file) {

  private var counter = 0L

  def writeEntry(name: String)(f: (OutputStream => Unit)) {
    val bos = newOutputStream(name)
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

  def newOutputStream(name: String): OutputStream = {
    val out = new TFileOutputStream(new TFile(tfile, name))
    val bos = new BufferedOutputStream(out, 20 * 1024)
    new CountingOutputStream(bos)
  }

  def size() = {
    counter
  }

  def writeJson[T](name: String, t: T)(implicit m: Manifest[T]) {
    writeEntry(name) { o =>
      val js = new JsonSerialization()
      js.writeObject(t, o)
    }
  }

  def writeManifest(x: BackupFolderConfiguration) {
    val versionNumber: String = version.BuildInfo.version
    val m = new MetaInfo(x.fileManager.getDateFormatted, versionNumber)
    writeJson("manifest.txt", m)
  }

}

case class MetaInfo(date: String, writingVersion: String)