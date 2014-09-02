package ch.descabato.utils

import java.io.PrintStream
import javax.xml.bind.DatatypeConverter
import java.text.DecimalFormat
import com.typesafe.scalalogging.LazyLogging
import java.io.File
import java.nio.file.Files
import java.nio.ByteBuffer
import java.io.OutputStream
import sun.nio.ch.DirectBuffer
import ch.descabato.ByteArrayOutputStream
import scala.collection.mutable
import java.io.InputStream
import scala.language.implicitConversions
import ch.descabato.core.BaWrapper

object Utils extends LazyLogging {
  
  private val units = Array[String]("B", "KB", "MB", "GB", "TB");
  def isWindows = System.getProperty("os.name").contains("indows")
  def readableFileSize(size: Long, afterDot: Int = 1): String = {
    if (size <= 0) return "0";
    val digitGroups = (Math.log10(size) / Math.log10(1024)).toInt;
    val afterDotPart = if (afterDot == 0) "#" else "0" * afterDot
    return new DecimalFormat("#,##0." + afterDotPart).format(size / Math.pow(1024, digitGroups)) + Utils.units(digitGroups);
  }

  private def encodeBase64(bytes: Array[Byte]) = DatatypeConverter.printBase64Binary(bytes);
  private def decodeBase64(s: String) = DatatypeConverter.parseBase64Binary(s);

  def encodeBase64Url(bytes: Array[Byte]) = encodeBase64(bytes).replace('+', '-').replace('/', '_')
  def decodeBase64Url(s: String) = decodeBase64(s.replace('-', '+').replace('_', '/'));

  def normalizePath(x: String) = x.replace('\\', '/')

  def logException(t: Throwable) {
    val baos = new ByteArrayOutputStream()
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
    logger.debug(baos.toString())
    baos.recycle()
  }

}

object Implicits {
  import scala.language.higherKinds
  implicit def byteArrayToWrapper(a: Array[Byte]) = new BaWrapper(a)
  
  implicit class ByteBufferUtils(buf: ByteBuffer) {
    def writeTo(out: OutputStream) {
      if (buf.hasArray()) {
        out.write(buf.array(), buf.position(), buf.remaining())
      } else {
        throw new UnsupportedOperationException("Not yet implemented, but easy to do")
      }
    }

    def recycle() {
      buf match {
        case x: DirectBuffer => x.cleaner().clean()
        case x if x.hasArray() => ObjectPools.byteArrayPool.recycle(x.array())
      }
    }

    def toArray() = {
      if (buf.hasArray()) {
        val out = new Array[Byte](buf.limit())
        System.arraycopy(buf.array(), buf.arrayOffset(), out, 0, buf.remaining())
        out
      } else {
        throw new UnsupportedOperationException("Not yet implemented, but easy to do")
      }
    }
  }
  implicit class InvariantContains[T, CC[X] <: Seq[X]](xs: CC[T]) {
    def safeContains(x: T): Boolean = xs contains x
  }
  implicit class InvariantContains2[T, CC[X] <: scala.collection.Set[X]](xs: CC[T]) {
    def safeContains(x: T): Boolean = xs contains x
  }
  implicit class InvariantContains3[T](xs: scala.collection.Map[T, _]) {
    def safeContains(x: T): Boolean = xs.keySet contains x
  }

  implicit class InputStreamBetter(in: InputStream) {
    def readFully() = {
      val baos = new ByteArrayOutputStream()
      Streams.copy(in, baos)
      baos.toByteArray()
    }
  }
}

object FileUtils extends Utils {
  def getRelativePath(dest: File, to: File, path: String) = {
    // Needs to take common parts out of the path.
    // Different semantics on windows. Upper-/lowercase is ignored, ':' may not be part of the output
    def prepare(f: File) = {
      var path = if (Files.isSymbolicLink(f.toPath)) f.getAbsolutePath() else f.getCanonicalPath()
      if (Utils.isWindows)
        path = path.replaceAllLiterally("\\", "/")
      path.split("/").toList
    }
    val files = (prepare(to), prepare(new File(path)))

    def compare(s1: String, s2: String) = if (Utils.isWindows) s1.equalsIgnoreCase(s2) else s1 == s2

    def cutFirst(files: (List[String], List[String])): String = {
      files match {
        case (x :: xTail, y :: yTail) if compare(x, y) => cutFirst(xTail, yTail)
        case (_, x) => x.mkString("/")
      }
    }
    val cut = cutFirst(files)
    def cleaned(s: String) = if (Utils.isWindows) s.replaceAllLiterally(":", "_") else s
    new File(dest, cleaned(cut))
  }

  def deleteAll(f: File) = {
    def walk(f: File) {
      f.isDirectory() match {
        case true =>
          f.listFiles().toList.foreach(walk);
          f.delete()
        case false =>
          f.delete()
          Files.deleteIfExists(f.toPath())
      }
    }
    var i = 0
    do {
      walk(f)
      i += 1
      Thread.sleep(500)
    } while (i < 5 && f.exists)
    if (i > 1) {
      logger.warn(s"Took delete all $i runs, now folder is deleted "+(!f.exists))
    }
  }

}

trait Utils extends LazyLogging {
  lazy val l = logger

  def readableFileSize(size: Long): String = Utils.readableFileSize(size)

  def logException(t: Throwable) {
    Utils.logException(t)
  }

}
