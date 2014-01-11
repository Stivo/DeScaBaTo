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

abstract class ZipFileHandler(zip: File) 

/**
 * A thin wrapper around a java zip file reader.
 * Needs to be closed in the end.
 */
class ZipFileReader(zip: File) extends ZipFileHandler(zip) {

  val zf = new JZipFile(zip);
  
  def this(s: String) = this(new File(s))  
    
  private val _zipEntries = zf.entries().toArray
   
  val names = _zipEntries.map(_.getName())
   
  def getStream(name: String) = zf.getInputStream(zf.getEntry(name))
   
  def close() {
    zf.close()
  }

}

/**
 * A thin wrapper around a zip file writer.
 * Has the current file size.
 */
class ZipFileWriter(zip: File) extends ZipFileHandler(zip) {
  
  val fos = new CountingOutputStream(new FileOutputStream(zip))
  val out = new ZipOutputStream(fos)
  
  // compression is done already before
  out.setLevel(ZipEntry.STORED)
  
  def writeEntry(name: String, content: Array[Byte]) {
    out.putNextEntry(new ZipEntry(name))
    out.write(content)
    out.closeEntry()
  }
  
  def close() {
    out.close()
  }
  
  def size() = {
    fos.count
  }
  
}
