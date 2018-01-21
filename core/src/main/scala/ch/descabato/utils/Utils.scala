package ch.descabato.utils

import java.io._
import java.nio.ByteBuffer
import java.nio.file.Files
import java.security.MessageDigest
import java.text.DecimalFormat
import java.util
import javax.xml.bind.DatatypeConverter

import ch.descabato.CustomByteArrayOutputStream
import ch.descabato.core_old.BackupFolderConfiguration
import com.typesafe.scalalogging.{LazyLogging, Logger}

import scala.collection.mutable
import scala.language.implicitConversions

trait RealEquality[T] {
  def ===(t: T): Boolean
  def !==(t: T): Boolean = !(this === t)
}

object Hash {
  def isDefined (hash: Hash): Boolean = {
    hash.bytes != null && hash.length > 0
  }

  val nul = new Hash(Array.ofDim[Byte](0))

  def apply(hash: Array[Byte]) = {
    new Hash(hash)
  }

}

class Hash(val bytes: Array[Byte]) extends AnyVal {
  def length: Int = bytes.length
  def base64: String = Utils.encodeBase64Url(bytes)
  def ===(other: Hash): Boolean = java.util.Arrays.equals(bytes, other.bytes)
  def !==(other: Hash): Boolean = !(this === other)
  def wrap(): BytesWrapper = new BytesWrapper(bytes)
  def grouped(config: BackupFolderConfiguration): Iterator[Array[Byte]] = bytes.grouped(config.hashLength)
}

object BytesWrapper {
  def apply(bytes: Array[Byte]): BytesWrapper = {
    new BytesWrapper(bytes)
  }
}

class BytesWrapper(val array: Array[Byte], var offset: Int = 0, var length: Int = -1) {

  def asInputStream() = new ByteArrayInputStream(array, offset, length)

  if (length == -1) {
    length = array.length - offset
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
    true
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
    val end = offset + length
    while (i < end) {
     result = 31 * result + array(i)
      i += 1
    }
    result
  }
  override def toString(): String = array.length + ": " + new String(array)
  def toByteBuffer(): ByteBuffer = ByteBuffer.wrap(array, offset, length)
}

object Utils extends LazyLogging {
  
  private val units = Array[String]("B", "KB", "MB", "GB", "TB")
  def isWindows: Boolean = System.getProperty("os.name").contains("indows")
  def readableFileSize(size: Long, afterDot: Int = 1): String = {
    if (size <= 0) return "0"
    val digitGroups = (Math.log10(size) / Math.log10(1024)).toInt
    val afterDotPart = if (afterDot == 0) "#" else "0" * afterDot
    new DecimalFormat("#,##0. " + afterDotPart).format(size / Math.pow(1024, digitGroups)) + Utils.units(digitGroups)
  }

  private def encodeBase64(bytes: Array[Byte]) = DatatypeConverter.printBase64Binary(bytes)
  private def decodeBase64(s: String) = DatatypeConverter.parseBase64Binary(s)

  def encodeBase64Url(bytes: Array[Byte]): String = encodeBase64(bytes).replace('+', '-').replace('/', '_')
  def decodeBase64Url(s: String): Array[Byte] = decodeBase64(s.replace('-', '+').replace('_', '/'))

  def normalizePath(x: String): String = x.replace('\\', '/')

  def logException(t: Throwable) {
    val baos = new CustomByteArrayOutputStream()
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
  implicit def hashToWrapper(a: Hash): BytesWrapper = new BytesWrapper(a.bytes)
  implicit def hashToArray(a: Hash): Array[Byte] = a.bytes
  implicit class AwareMessageDigest(md: MessageDigest) {
    def update(bytesWrapper: BytesWrapper): Unit = {
      md.update(bytesWrapper.array, bytesWrapper.offset, bytesWrapper.length)
    }
    def digest(bytesWrapper: BytesWrapper): Hash = {
      update(bytesWrapper)
      digest()
    }
    def digest(): Hash = {
      new Hash(md.digest())
    }
  }
  implicit class AwareOutputStream(os: OutputStream) {
    def write(bytesWrapper: BytesWrapper) {
      os.write(bytesWrapper.array, bytesWrapper.offset, bytesWrapper.length)
    }
  }
  implicit class ByteArrayUtils(buf: Array[Byte]) extends RealEquality[Array[Byte]]{
    def ===(other: Array[Byte]): Boolean = java.util.Arrays.equals(buf, other)
    def wrap(): BytesWrapper = new BytesWrapper(buf)
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
  def getRelativePath(dest: File, to: File, path: String): File = {
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

  def deleteAll(f: File): Unit = {
    def walk(f: File) {
      if (f.isDirectory()) {
        f.listFiles().toList.foreach(walk)
        f.delete()
      } else {
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
  lazy val l: Logger = logger

  def readableFileSize(size: Long): String = Utils.readableFileSize(size)

  def logException(t: Throwable) {
    Utils.logException(t)
  }

}

class ByteArrayMap[T] extends mutable.HashMap[Array[Byte], T] {
  override protected def elemHashCode(key: Array[Byte]): Int = {
    util.Arrays.hashCode(key)
  }

  override protected def elemEquals(key1: Array[Byte], key2: Array[Byte]): Boolean = {
    util.Arrays.equals(key1, key2)
  }
}

class FastHashMap[T] extends mutable.HashMap[Hash, T] {
  override protected def elemHashCode(key: Hash): Int = {
    util.Arrays.hashCode(key.bytes)
  }

  override protected def elemEquals(key1: Hash, key2: Hash): Boolean = {
    util.Arrays.equals(key1.bytes, key2.bytes)
  }
}