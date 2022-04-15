package ch.descabato.utils

import ch.descabato.CustomByteArrayOutputStream
import ch.descabato.utils.FastHashMap.W
import com.google.protobuf.ByteString
import com.typesafe.scalalogging.LazyLogging
import com.typesafe.scalalogging.Logger
import org.bouncycastle.crypto.Digest
import org.bouncycastle.util.encoders.Hex
import scalapb.TypeMapper

import java.io._
import java.nio.file.Files
import java.security.MessageDigest
import java.text.DecimalFormat
import java.util
import java.util.Base64
import scala.collection.mutable
import scala.language.implicitConversions

trait RealEquality[T] {
  def ===(t: T): Boolean

  def !==(t: T): Boolean = !(this === t)
}

object Hash {
  val empty: Hash = Hash(Array.ofDim(0))

  def fromBase64(hash: String): Hash = {
    Hash(Utils.decodeBase64Url(hash))
  }

  def isDefined(hash: Hash): Boolean = {
    hash.bytes != null && hash.length > 0
  }

  def apply(hash: Array[Byte]): Hash = {
    new Hash(hash)
  }

  implicit val typeMapper: TypeMapper[ByteString, Hash] =
    TypeMapper[ByteString, Hash](b => Hash(b.toByteArray))(_.wrap().toProtobufByteString())

}

class Hash private(val bytes: Array[Byte]) extends AnyVal {
  def length: Int = bytes.length

  def base64: String = Utils.encodeBase64Url(bytes)

  def ===(other: Hash): Boolean = java.util.Arrays.equals(bytes, other.bytes)

  def !==(other: Hash): Boolean = !(this === other)

  def wrap(): BytesWrapper = BytesWrapper(bytes)

  def hashContent(): Int = util.Arrays.hashCode(bytes)

  override def toString: String = {
    "0x" + Hex.toHexString(bytes)
  }
}

object BytesWrapper {

  implicit val typeMapper: TypeMapper[ByteString, BytesWrapper] =
    TypeMapper[ByteString, BytesWrapper](b => BytesWrapper.apply(b.toByteArray))(Option(_).map(_.toProtobufByteString()).getOrElse(ByteString.EMPTY))

  def apply(bytes: Array[Byte], offset: Int = 0, length: Int = -1): BytesWrapper = {
    require(bytes != null)
    require(length <= bytes.length || length < 0)
    val correctLength = if (length < 0) bytes.length - offset else length
    new BytesWrapper(bytes, offset, correctLength)
  }

  val empty: BytesWrapper = BytesWrapper(Array.emptyByteArray, 0, 0)
}

class BytesWrapper private(val array: Array[Byte], val offset: Int, val length: Int) extends RealEquality[BytesWrapper] {

  def asInputStream() = new ByteArrayInputStream(array, offset, length)

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

  def copyToArray(b: Array[Byte], off: Int, toRead: Int) = {
    System.arraycopy(array, offset, b, off, toRead)
  }

  def toProtobufByteString(): ByteString = {
    ByteString.copyFrom(array, offset, length)
  }

  def equals(other: BytesWrapper): Boolean = {
    if (this.eq(other)) {
      return true
    }
    if (this.length != other.length) {
      return false
    }
    if (this.hashCode != other.hashCode) {
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

  override lazy val hashCode: Int = {
    if (array == null) {
      0
    } else {
      var result: Int = 1
      var i = offset
      val end = offset + length
      while (i < end) {
        result = 31 * result + array(i)
        i += 1
      }
      result
    }
  }

  override def toString(): String = s"${array.length}: ${new String(array)}"

  override def ===(t: BytesWrapper): Boolean = equals(t)
}

object Utils extends LazyLogging {

  private val units = Array[String]("B", "KiB", "MiB", "GiB", "TiB")

  def isWindows: Boolean = System.getProperty("os.name").contains("indows")

  def readableFileSize(size: Long, afterDot: Int = 1): String = {
    if (size <= 0) return "0"
    val digitGroups = (Math.log10(size.toDouble) / Math.log10(1024)).toInt
    val afterDotPart = if (afterDot == 0) "#" else "0" * afterDot
    new DecimalFormat("#,##0. " + afterDotPart).format(size / Math.pow(1024, digitGroups)) + Utils.units(digitGroups)
  }

  def encodeBase64(bytes: Array[Byte]): String = Base64.getEncoder.encodeToString(bytes)

  def decodeBase64(s: String): Array[Byte] = Base64.getDecoder.decode(s)

  def encodeBase64Url(bytes: Array[Byte]): String = encodeBase64(bytes).replace('+', '-').replace('/', '_')

  def decodeBase64Url(s: String): Array[Byte] = decodeBase64(s.replace('-', '+').replace('_', '/'))

  def normalizePath(x: String): String = x.replace('\\', '/')

  def logException(t: Throwable): Unit = {
    val baos = new CustomByteArrayOutputStream()
    val ps = new PrintStream(baos)

    def print(t: Throwable): Unit = {
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

  def formatDuration(nanos: Long): String = {
    val millis = nanos / 1000 / 1000
    val seconds = millis / 1000
    if (seconds == 0) {
      f"${millis} ms"
    } else if (seconds < 10) {
      f"${millis * 0.001}%02.1f seconds"
    } else {
      val minutes = seconds / 60
      val hours = minutes / 60
      val days = hours / 24
      val add = if (days == 0) "" else s"$days days "
      f"$add${hours % 24}%02d:${minutes % 60}%02d:${seconds % 60}%02d"
    }
  }

}

object Implicits {

  import scala.language.higherKinds

  implicit def hashToWrapper(a: Hash): BytesWrapper = BytesWrapper(a.bytes)

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
      Hash(md.digest())
    }
  }

  implicit class AwareDigest(md: Digest) {
    def update(bytesWrapper: BytesWrapper): Unit = {
      md.update(bytesWrapper.array, bytesWrapper.offset, bytesWrapper.length)
    }

    def digest(bytesWrapper: BytesWrapper): Hash = {
      update(bytesWrapper)
      digest()
    }

    def digest(): Hash = {
      val bytes = Array.ofDim[Byte](md.getDigestSize)
      md.doFinal(bytes, 0)
      Hash(bytes)
    }
  }

  implicit class AwareOutputStream(os: OutputStream) {
    def write(bytesWrapper: BytesWrapper): Unit = {
      os.write(bytesWrapper.array, bytesWrapper.offset, bytesWrapper.length)
    }
  }

  implicit class ByteArrayUtils(buf: Array[Byte]) extends RealEquality[Array[Byte]] {
    def ===(other: Array[Byte]): Boolean = java.util.Arrays.equals(buf, other)

    def wrap(): BytesWrapper = BytesWrapper(buf)
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
        path = path.replace("\\", "/")
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

    def cleaned(s: String) = if (Utils.isWindows) s.replace(":", "_") else s

    new File(dest, cleaned(cut))
  }

  def deleteAll(f: File): Unit = {
    def walk(f: File): Unit = {
      if (f.isDirectory()) {
        f.listFiles().toList.foreach(walk)
        f.delete()
      } else {
        f.delete()
        Files.deleteIfExists(f.toPath())
      }
    }

    var i = 0
    while ( {
      {
        walk(f)
        i += 1
        Thread.sleep(500)
      };
      i < 5 && f.exists
    }) ()
    if (i > 1) {
      logger.warn(s"Took delete all $i runs, now folder is deleted " + (!f.exists))
    }
  }

}

trait Utils extends LazyLogging {
  lazy val l: Logger = {
    //    println(s"Requesting logger for ${getClass}")
    logger
  }

  def readableFileSize(size: Long): String = Utils.readableFileSize(size)

  def logException(t: Throwable): Unit = {
    Utils.logException(t)
  }

}

class FastHashMap[T] extends mutable.AbstractMap[Hash, T] {

  private val map: mutable.HashMap[W, T] = new mutable.HashMap()

  override def get(key: Hash): Option[T] = {
    map.get(new W(key.bytes))
  }

  override def subtractOne(elem: Hash): FastHashMap.this.type = {
    map.subtractOne(new W(elem.bytes))
    this
  }

  override def iterator: Iterator[(Hash, T)] = {
    map.iterator.map { case (wrapper, t) => (Hash(wrapper.array), t) }
  }

  override def addOne(elem: (Hash, T)): FastHashMap.this.type = {
    map.addOne((new W(elem._1.bytes), elem._2))
    this
  }
}

object FastHashMap {

  private class W(val array: Array[Byte]) {

    override def equals(obj: Any): Boolean =
      obj match {
        case other: W => util.Arrays.equals(this.array, other.array)
        case _ => false
      }

    override def hashCode: Int = {
      util.Arrays.hashCode(this.array)
    }

  }

}

object StopWatch {
  private def register(watch: StopWatch): Unit = {
    synchronized {
      stopwatches :+= watch
    }
  }

  private var stopwatches: List[StopWatch] = List.empty

  def report: String = stopwatches.map(_.format).mkString("\n")
}

class StopWatch(name: String) {
  StopWatch.register(this)
  private var accumulated = 0L
  private var invocations = 0L

  def measure[T](f: => T): T = {
    invocations += 1
    val start = System.nanoTime()
    val out = f
    accumulated += System.nanoTime() - start
    out
  }

  def format: String = {
    if (invocations > 0) {
      s"$name called ${invocations} times, took ${Utils.formatDuration(accumulated)}. ${accumulated / invocations / 1_000_000.0} ms / invocation"
    } else {
      s"$name was never called"
    }
  }
}