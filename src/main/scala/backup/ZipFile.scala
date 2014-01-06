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

class ZipFileReader(zip: File) extends ZipFileHandler(zip) {

  val zf = new JZipFile(zip);
  
  def this(s: String) = this(new File(s))  
    
  private val _zipEntries = zf.entries().toArray
   
  val names = _zipEntries.map(_.getName())
   
  def getBytes(name: String) = Option(zf.getEntry(name)).map(getContent)

  def getString(name: String, enc: String = "utf-8") = getBytes(name).map(x => new String(x, enc))
  
  def getStream(name: String) = zf.getInputStream(zf.getEntry(name))
   
   private def getContent(ze: ZipEntry) = {
     val input = zf.getInputStream(ze)
      val out = Buffer[Byte]()
      val byte = 0
      val buf = Array.ofDim[Byte](10240)
      while (input.available() > 0) {
        val newOffset = input.read(buf, 0, buf.length - 1)
        out ++= buf.slice(0, newOffset)
      }
      out.toArray
   }

}

class ZipFileWriter(zip: File) extends ZipFileHandler(zip) {
  
  val fos = new CountingOutputStream(new FileOutputStream(zip))
  val out = new ZipOutputStream(fos)
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
