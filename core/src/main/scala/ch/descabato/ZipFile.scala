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

abstract class ZipFileHandler(zip: File)

/**
 * A thin wrapper around a java zip file reader.
 * Needs to be closed in the end.
 */
class ZipFileReader(val file: File) extends ZipFileHandler(file) with Utils {

  def this(s: String) = this(new File(s))

  val tfile = new TFile(file);

  lazy val names = tfile.list().toArray

  def getStream(name: String) = {
    val e = new TFile(tfile, name);
    new TFileInputStream(e)
  }

  def getEntrySize(name: String) = new TFile(tfile, name).length()

  def close() {
    TVFS.umount(tfile)
  }

}

/**
 * A thin wrapper around a zip file writer.
 * Has the current file size.
 */
class ZipFileWriter(val file: File) extends ZipFileHandler(file) {

  private var counter = 0L

  val tfile = new TFile(file);

  // compression is done already before

  def writeEntry(name: String, f: (OutputStream => Unit)) {
    val out = new TFileOutputStream(new TFile(tfile, name))
    val o2 = new CountingOutputStream(out)
    try {
      f(o2)
    } finally {
      o2.close()
      counter += o2.counter
    }
  }

  def close() {
    TVFS.umount(tfile);
  }

  def size() = {
    counter
  }

}
