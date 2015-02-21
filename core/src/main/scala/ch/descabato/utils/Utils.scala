package ch.descabato.utils

import java.io.{File, InputStream, OutputStream, PrintStream}
import java.nio.ByteBuffer
import java.nio.file.Files
import java.security.MessageDigest
import java.text.DecimalFormat
import java.util
import javax.xml.bind.DatatypeConverter

import ch.descabato.ByteArrayOutputStream
import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.compress.utils.IOUtils
import sun.nio.ch.DirectBuffer

import scala.language.implicitConversions

class Hash(val bytes: Array[Byte]) extends AnyVal {
  def length = bytes.length
  def base64 = Utils.encodeBase64Url(bytes)
  def isNull = length == 0
  // minimal size of hash is 16
  def isNotNull = !isNull
  def safeEquals(other: Hash) = java.util.Arrays.equals(bytes, other.bytes)
}

object NullHash {
  val nul = new Hash(Array.ofDim[Byte](0))
}

class BytesWrapper(val array: Array[Byte], var offset: Int = 0, var length: Int = -1) {
  if (length == -1) {
    length = array.length
  }
  def apply(i: Int) = array(i + offset)
  def asArray(): Array[Byte] = {
    if (array == null) {
      Array.empty[Byte]
    } else {
      if (offset == 0 && length == array.length) {
        array
      } else {
        val out = Array.ofDim[Byte](length)
        System.arraycopy(array, offset, out, 0, length)
        out
      }
    }
  }
  def equals(other: BytesWrapper): Boolean = {
    if (this.length != other.length) {
      return false
    }
    var o1 = this.offset
    var o2 = other.offset
    val end1 = this.offset + this.length
    while (o1 < end1) {
      if (array(o1) != other.array(o2)) {
        return false
      }
      o1 += 1
      o2 += 1
    }
    return true
  }
  override def equals(obj: Any): Boolean =
    obj match {
      case other: BytesWrapper => equals(other)
      case _ => false
    }

  override def hashCode: Int = {
    if (array == null)
      return 0
    var result: Int = 1
    var i = offset
    var end = offset + length
    while (i < end) {
     result = 31 * result + array(i)
      i += 1
    }
    return result
  }
  override def toString() = array.length + ": " + new String(array)
  def toByteBuffer(): ByteBuffer = ByteBuffer.wrap(array, offset, length)
}

object Utils extends LazyLogging {
  
  private val units = Array[String]("B", "KB", "MB", "GB", "TB")
  def isWindows = System.getProperty("os.name").contains("indows")
  def readableFileSize(size: Long, afterDot: Int = 1): String = {
    if (size <= 0) return "0"
    val digitGroups = (Math.log10(size) / Math.log10(1024)).toInt
    val afterDotPart = if (afterDot == 0) "#" else "0" * afterDot
    new DecimalFormat("#,##0." + afterDotPart).format(size / Math.pow(1024, digitGroups)) + Utils.units(digitGroups)
  }

  private def encodeBase64(bytes: Array[Byte]) = DatatypeConverter.printBase64Binary(bytes)
  private def decodeBase64(s: String) = DatatypeConverter.parseBase64Binary(s)

  def encodeBase64Url(bytes: Array[Byte]) = encodeBase64(bytes).replace('+', '-').replace('/', '_')
  def decodeBase64Url(s: String) = decodeBase64(s.replace('-', '+').replace('_', '/'))

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
  }

}

object Implicits {
  import scala.language.higherKinds
  implicit def byteArrayToWrapper(a: Array[Byte]): BytesWrapper = new BytesWrapper(a)
  implicit def hashToWrapper(a: Hash): BytesWrapper = new BytesWrapper(a.bytes)
  implicit class AwareMessageDigest(md: MessageDigest) {
    def update(bytesWrapper: BytesWrapper): Unit = {
      md.update(bytesWrapper.array, bytesWrapper.offset, bytesWrapper.length)
    }
    def finish(bytesWrapper: BytesWrapper): Hash = {
      md.update(bytesWrapper.array, bytesWrapper.offset, bytesWrapper.length)
      new Hash(md.digest())
    }
    def finish(): Hash = {
      new Hash(md.digest())
    }
  }
  implicit class AwareOutputStream(os: OutputStream) {
    def write(bytesWrapper: BytesWrapper) {
      os.write(bytesWrapper.array, bytesWrapper.offset, bytesWrapper.length)
    }
  }
  implicit class AwareByteArray(array: Array[Byte]) {
    def wrap(): BytesWrapper = new BytesWrapper(array)
  }

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
          f.listFiles().toList.foreach(walk)
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
