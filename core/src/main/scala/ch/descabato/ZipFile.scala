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

abstract class ZipFileHandler(zip: File)

/**
 * A thin wrapper around a java zip file reader.
 * Needs to be closed in the end.
 */
class ZipFileReader(val file: File) extends ZipFileHandler(file) with Utils {

  class RegisteredInputStream(in: InputStream, val name: String) extends DelegatingInputStream(in) {
    streams += this
    override def close() = {
      super.close
      streams -= this
    }
  }

  private val streams = Buffer[RegisteredInputStream]()

  lazy val zf = new JZipFile(file);

  def this(s: String) = this(new File(s))

  private lazy val _zipEntries = zf.entries().toArray

  lazy val names = _zipEntries.map(_.getName())

  def getStream(name: String) = {
    val out = zf.getInputStream(zf.getEntry(name))
    val out2 = new RegisteredInputStream(out, name) 
    out2
  }

  def getEntrySize(name: String) = zf.getEntry(name).getSize()

  def close() {
    for (s <- streams) {
      s.close()
    }
    zf.close()
  }

}

/**
 * A thin wrapper around a zip file writer.
 * Has the current file size.
 */
class ZipFileWriter(val file: File) extends ZipFileHandler(file) {

  val fos = new CountingOutputStream(new Streams.UnclosedFileOutputStream(file))
  val out = new ZipOutputStream(fos)

  // compression is done already before
  out.setLevel(ZipEntry.STORED)

  def writeEntry(name: String, f: (OutputStream => Unit)) {
    out.putNextEntry(new ZipEntry(name))
    f(out)
    out.closeEntry()
  }

  def close() {
    out.close()
  }

  def size() = {
    fos.count
  }

}
