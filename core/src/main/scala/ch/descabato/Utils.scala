package ch.descabato

import java.io.PrintStream
import javax.xml.bind.DatatypeConverter
import java.text.DecimalFormat
import com.typesafe.scalalogging.slf4j.Logging
import net.java.truevfs.access.TFile
import net.java.truevfs.access.TVFS

object Utils extends Logging {

  private val units = Array[String]("B", "KB", "MB", "GB", "TB");
  def isWindows = System.getProperty("os.name").contains("indows")
  def readableFileSize(size: Long, afterDot: Int = 1): String = {
    if (size <= 0) return "0";
    val digitGroups = (Math.log10(size) / Math.log10(1024)).toInt;
    val afterDotPart = if (afterDot == 0) "#" else "0" * afterDot
    return new DecimalFormat("#,##0." + afterDotPart).format(size / Math.pow(1024, digitGroups)) + Utils.units(digitGroups);
  }

  def encodeBase64(bytes: Array[Byte]) = DatatypeConverter.printBase64Binary(bytes);
  def decodeBase64(s: String) = DatatypeConverter.parseBase64Binary(s);

  def encodeBase64Url(bytes: Array[Byte]) = encodeBase64(bytes).replace('+', '-').replace('/', '_')
  def decodeBase64Url(s: String) = decodeBase64(s.replace('-', '+').replace('_', '/'));

  def normalizePath(x: String) = x.replace('\\', '/')

  def logException(t: Throwable) {
    ObjectPools.baosPool.withObject(Unit, { baos =>
      val ps = new PrintStream(baos)
      def print(t: Throwable) {
        t.printStackTrace(ps)
        if (t.getCause() != null) {
          ps.println()
          ps.println("Caused by: ")
          print(t.getCause())
        }
      }
      print(t)
      logger.debug(new String(baos.toByteArray))
    })
  }
  
  def closeTFile(x: TFile) {
    TVFS.umount(x.getEnclArchive())
  }

}

trait Utils extends Logging {
  lazy val l = logger
  import Utils._

  def readableFileSize(size: Long): String = Utils.readableFileSize(size)

  def logException(t: Throwable) {
    Utils.logException(t)
  }
  
}