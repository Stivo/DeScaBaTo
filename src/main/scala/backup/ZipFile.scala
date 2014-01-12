package backup

import java.io.File
import scala.collection.JavaConversions._
import java.util.zip.{ZipFile => JZipFile}
import java.util.zip.ZipEntry
import scala.collection.mutable.Buffer
import java.io.BufferedInputStream
import java.util.zip.ZipOutputStream
import java.io.FileInputStream
import java.io.FileOutputStream
import backup.Streams.CountingOutputStream
import java.io.OutputStream

abstract class ZipFileHandler(zip: File) 

/**
 * A thin wrapper around a java zip file reader.
 * Needs to be closed in the end.
 */
class ZipFileReader(val file: File) extends ZipFileHandler(file) {

  lazy val zf = new JZipFile(file);
  
  def this(s: String) = this(new File(s))  
    
  private lazy val _zipEntries = zf.entries().toArray
   
  lazy val names = _zipEntries.map(_.getName())
   
  def getStream(name: String) = zf.getInputStream(zf.getEntry(name))
 
  def getEntrySize(name: String) = zf.getEntry(name).getSize()
  
  def close() {
    zf.close()
  }

}

/**
 * A thin wrapper around a zip file writer.
 * Has the current file size.
 */
class ZipFileWriter(val file: File) extends ZipFileHandler(file) {
  
  val fos = new CountingOutputStream(new FileOutputStream(file))
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
