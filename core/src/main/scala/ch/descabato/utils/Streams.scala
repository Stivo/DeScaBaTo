package ch.descabato.utils

import java.io.InputStream
import java.io.OutputStream
import java.security.MessageDigest
import scala.collection.mutable.Buffer
import java.util.Arrays
import java.io.File
import scala.ref.WeakReference
import scala.concurrent.duration._
import scala.collection.mutable.SynchronizedSet
import scala.collection.mutable.HashSet
import java.io.FileOutputStream
import java.util.zip.ZipException
import org.tukaani.xz.XZIOException
import org.apache.commons.compress.compressors.CompressorException
import java.io.IOException
import ch.descabato.core.BackupCorruptedException
import ch.descabato.ByteArrayOutputStream
import ch.descabato.frontend.ProgressReporters

object ObjectPools extends Utils {

  var foundExactCounter = 0
  var foundExactRequests = 0
  var foundMinimumCounter = 0
  var foundMinimumRequests = 0

  def printStatistics(size: Int) {
//    l.info("Found minimum "+foundMinimumCounter+" / "+foundMinimumRequests)
//    l.info("Found exact "+foundExactCounter+" / "+foundExactRequests)
//    l.info("Current Weak References "+size)
  }
  
  class ByteArrayObjectPool {
    type T = Array[Byte]
    val local = new ThreadLocal[Buffer[WeakReference[T]]]
    def getStack() = {
      val out = local.get()
      if (out == null) {
        val n = Buffer[WeakReference[T]]()
        local.set(n)
        n
      } else {
        out
      }
    }
    final def getMinimum(arg: Int): T = {
      def isValidForArg(x: T) = x.length >= arg
      if (arg < 1024) {
        return makeNew(arg)
      }
      val found = get(isValidForArg)
//      l.info(s"Found byte array with minimum $arg ${found.isDefined}")
      if (found.isDefined) {
        foundMinimumCounter += 1
      }
      foundMinimumRequests += 1
      if (foundMinimumRequests % 1000 == 0) {
        printStatistics(getStack.size)
      }
      found getOrElse (makeNew(arg))
    }

    final def getExactly(arg: Int): T = {
      def isValidForArg(x: T) = x.length == arg
      if (arg < 1024) {
        return makeNew(arg)
      }
      val found = get(isValidForArg)
//      l.info(s"Found byte array with exactly $arg ${found.isDefined}")
      if (found.isDefined) {
        foundExactCounter += 1
      }
      foundExactRequests += 1
      found getOrElse (makeNew(arg))
    }

    private def get(f: T => Boolean): Option[T] = {
      val stack = getStack()
      var strongRef: Option[T] = None
      while (!stack.isEmpty) {
        val toRemove = Buffer[WeakReference[T]]()
        val result = stack.find {
          case wr@WeakReference(x) if (f(x)) =>
            // before x is garbage collected, save it to a strong reference
            strongRef = Some(x)
            toRemove += wr;
            true
          case WeakReference(x) => false // continue searching
          case wr => // This is an empty reference now, we might as well delete it
            toRemove += wr; false
        }
        stack --= toRemove
        return strongRef
      }
      None
    }


    final def recycle(t: T) {
//      val stack = getStack
//      stack += (WeakReference(t))
    }
    protected def makeNew(arg: Int): T = {
      Array.ofDim[Byte](arg)
    }
  }

  val byteArrayPool = new ByteArrayObjectPool

}

object Streams extends Utils {

  def readFrom(in: InputStream, f: (Array[Byte], Int) => Unit) {
    val buf = ObjectPools.byteArrayPool.getMinimum(100*1024)
    try {
      var lastRead = 1
      while (lastRead > 0) {
        lastRead = in.read(buf)
        if (lastRead > 0) {
          f(buf, lastRead)
        }
      }
    } finally {
      in.close()
      ObjectPools.byteArrayPool.recycle(buf)
    }
  }

  def copy(in: InputStream, out: OutputStream, progress: Option[Int => Unit] = None) {
    try {
      readFrom(in, { (x: Array[Byte], len: Int) =>
        out.write(x, 0, len)
        for (p <- progress)
          p(len)
      })
    } finally {
      in.close()
      out.close()
    }
  }

  class SplitInputStream(in: InputStream, outStreams: List[OutputStream]) extends InputStream {
    def read(): Int = throw new IllegalAccessException("This method should not be used")
    def readComplete() {
      try {
        readFrom(in, { (buf: Array[Byte], len: Int) =>
          for (outStream <- outStreams) {
            outStream.write(buf, 0, len)
          }
        })
      } finally {
        outStreams.foreach(_.close)
        close()
      }
    }
    override def close() = in.close()
  }

  class DelegatingInputStream(in: InputStream) extends InputStream {
    def read() = in.read()
    override def read(b: Array[Byte], start: Int, len: Int) = in.read(b, start, len)
    override def close() = in.close()
    override def mark(limit: Int) = in.mark(limit)
    override def reset() = in.reset()
    override def markSupported() = in.markSupported()
  }

  class DelegatingOutputStream(out: OutputStream*) extends OutputStream {
    def write(b: Int) = out foreach(_.write(b))
    override def write(b: Array[Byte], start: Int, len: Int) = out foreach(_.write(b, start, len))
    override def close() = out foreach(_.close())
    override def flush() = out foreach(_.flush())
  }

  abstract class HashingInputStream(in: InputStream, messageDigest: MessageDigest) extends DelegatingInputStream(in) {
    var finished = false
    override def read() = {
      val out = in.read()
      if (out >= 0)
        messageDigest.update(out.toByte)
      else
        finished = true
      out
    }
    override def read(b: Array[Byte], start: Int, len: Int) = {
      val out = super.read(b, start, len)
      if (out > 0)
        messageDigest.update(b, start, out)
      if (out < 0)
        finished = true
      out
    }
    override def close() {
      super.close
      if (finished)
        // Stream has been read to the end, so the verification should run 
        hashComputed(messageDigest.digest())
    }

    def hashComputed(hash2: Array[Byte]): Unit

    override def markSupported() = false
  }

  class VerifyInputStream(in: InputStream, messageDigest: MessageDigest, hash: Array[Byte], file: File)
    extends HashingInputStream(in, messageDigest) {
    final def hashComputed(hash2: Array[Byte]) {
      if (!Arrays.equals(hash, hash2)) {
        verificationFailed(hash2)
      }
    }
    def verificationFailed(hash2: Array[Byte]) {
      l.warn("Hash should be "+Utils.encodeBase64Url(hash)+" but is "+Utils.encodeBase64Url(hash2))
      throw new BackupCorruptedException(file)
    }
  }

  class ExceptionCatchingInputStream(in: InputStream, file: File) extends DelegatingInputStream(in) {
    override def read(buf: Array[Byte], start: Int, len: Int) = {
      try {
        super.read(buf, start, len)
      } catch {
        case z @ (_: ZipException | _: XZIOException | _: CompressorException) => 
        	throw new BackupCorruptedException(file, false).initCause(z)
        case z: IOException if (z.getStackTrace().head.getClassName().contains("bzip")) => throw new BackupCorruptedException(file, false).initCause(z)
      }
    }
  }

  class HashingOutputStream(val algorithm: String) extends OutputStream {
    val md = MessageDigest.getInstance(algorithm)

    var out: Option[Array[Byte]] = None

    def write(b: Int) {
      md.update(b.toByte);
    }

    override def write(buf: Array[Byte], start: Int, len: Int) {
      md.update(buf, start, len);
    }

    override def close() {
      out = Some(md.digest())
      super.close()
    }

  }

  class CountingOutputStream(val stream: OutputStream) extends DelegatingOutputStream(stream) {
    var counter: Long = 0
    override def write(b: Int) {
      counter += 1
      super.write(b)
    }

    override def write(buf: Array[Byte], start: Int, len: Int) {
      counter += len
      super.write(buf, start, len)
    }

    def count() = counter
  }

  class BlockOutputStream(val blockSize: Int, func: (Array[Byte] => _)) extends OutputStream {

    var out = new ByteArrayOutputStream(blockSize)

    var funcWasCalledOnce = false

    def write(b: Int) {
      out.write(b)
      handleEnd()
    }

    def handleEnd() {
      if (cur == blockSize) {
        funcWasCalledOnce = true
        func(out.toByteArray())
        out.reset()
      }
    }

    def cur = out.size()

    override def write(buf: Array[Byte], start: Int, len: Int) {
      var (lenC, startC) = (len, start)
      while (lenC > 0) {
        val now = Math.min(blockSize - cur, lenC)
        out.write(buf, startC, now)
        lenC -= now
        startC += now
        handleEnd()
      }
    }

    override def close() {
      if (out.size() > 0 || !funcWasCalledOnce)
        func(out.toByteArray())
      super.close()
      out.recycle()
      // out was returned to the pool, so remove instance pointer
      out = null
    }

  }

  class ReportingOutputStream(val out: OutputStream, val message: String,
    val interval: FiniteDuration = 5.seconds, var size: Long = -1) extends CountingOutputStream(out) {
    override def write(buf: Array[Byte], start: Int, len: Int) {
      val append = if (size > 1) {
        s"/${Utils.readableFileSize(size, 2)} ${(100 * count / size).toInt}%"
      } else ""
      ProgressReporters.reporter.ephemeralMessage(s"$message ${Utils.readableFileSize(count)}$append")
      super.write(buf, start, len)
    }
  }

}
