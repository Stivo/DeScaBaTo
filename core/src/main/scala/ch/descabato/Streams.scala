package ch.descabato;

import java.io.InputStream
import java.io.OutputStream
import java.security.MessageDigest
import scala.collection.mutable.Buffer
import java.util.Arrays
import java.io.ByteArrayOutputStream
import java.io.ByteArrayInputStream
import java.io.FileInputStream
import java.util.zip.GZIPOutputStream
import java.util.zip.GZIPInputStream
import java.io.File
import scala.collection.mutable.Stack
import scala.ref.WeakReference
import scala.reflect.ClassTag
import scala.concurrent.duration._
import org.tukaani.xz.XZOutputStream
import org.tukaani.xz.XZInputStream
import org.tukaani.xz.LZMA2Options
import scala.collection.mutable.SynchronizedSet
import scala.collection.mutable.HashSet
import java.io.FileOutputStream

object ObjectPools {

  trait Arg[T <: AnyRef, A] {
    def isValidForArg(x: T, arg: A): Boolean
  }

  trait NoneArg[T <: AnyRef] extends Arg[T, Unit] {
    self: ObjectPool[T, Unit] =>

    def isValidForArg(x: T, arg: Unit) = true
    def get(): T = get(())
    def makeNew(): T = get(())
  }

  trait ArrayArg[A] extends Arg[Array[A], Int] {
    self: ObjectPool[Array[A], Int] =>
    def classTag: ClassTag[A]
    def isValidForArg(x: Array[A], arg: Int) = x.length >= arg
    def makeNew(arg: Int) = Array.ofDim[A](arg)(classTag)
  }

  abstract class ObjectPool[T <: AnyRef, A] {
    self: Arg[T, A] =>
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
    final def get(arg: A): T = {
      val stack = getStack()
      while (!stack.isEmpty) {
        val toRemove = Buffer[WeakReference[T]]()
        val result = stack.find(_ match {
          case wr @ WeakReference(x) if (isValidForArg(x, arg)) =>
            toRemove += wr; true
          case WeakReference(x) => false // continue searching  
          case wr => // This is an empty reference now, we might as well delete it 
            toRemove += wr; false
        })
        stack --= toRemove
        result match {
          case Some(WeakReference(x)) => return x
          case _ =>
        }
      }
      makeNew(arg)
    }
    final def recycle(t: T) {
      val stack = getStack
      reset(t)
      stack += (WeakReference(t))
    }
    protected def reset(t: T) {}
    protected def makeNew(arg: A): T

    def withObject[R](arg: A, f: T => R): R = {
      val tObj = get(arg)
      try {
        f(tObj)
      } finally {
        recycle(tObj)
      }
    }

  }

  val baosPool = new ObjectPool[ByteArrayOutputStream, Unit] with NoneArg[ByteArrayOutputStream] {
    override def reset(t: ByteArrayOutputStream) = t.reset()
    def makeNew(arg: Unit) = new ByteArrayOutputStream(1024 * 1024 + 10)
  }

  val byteArrayPool = new ObjectPool[Array[Byte], Int] with ArrayArg[Byte] {
    def classTag = ClassTag.Byte
  }

}

object Streams extends Utils {
  import ObjectPools.baosPool

  def readFrom(in: InputStream, f: (Array[Byte], Int) => Unit) {
    val buf = Array.ofDim[Byte](10240)
    var lastRead = 1
    while (lastRead > 0) {
      lastRead = in.read(buf)
      if (lastRead > 0) {
        f(buf, lastRead)
      }
    }
    in.close()
  }

  def copy(in: InputStream, out: OutputStream) {
    try {
      readFrom(in, { (x: Array[Byte], len: Int) =>
        out.write(x, 0, len)
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

  class DelegatingOutputStream(out: OutputStream) extends OutputStream {
    def write(b: Int) = out.write(b)
    override def write(b: Array[Byte], start: Int, len: Int) = out.write(b, start, len)
    override def close() = out.close()
    override def flush() = out.flush()
  }

  class HashingInputStream(in: InputStream, messageDigest: MessageDigest, f: (Array[Byte] => _)) extends DelegatingInputStream(in) {
    override def read() = {
      val out = in.read()
      if (out >= 0)
        messageDigest.update(out.toByte)
      out
    }
    override def read(b: Array[Byte], start: Int, len: Int) = {
      val out = super.read(b, start, len)
      if (out > 0)
        messageDigest.update(b, start, out)
      out
    }
    override def close() = {
      super.close
      f(messageDigest.digest())
    }
    override def markSupported() = false
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

    var out = baosPool.get

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
      baosPool.recycle(out)
      // out was returned to the pool, so remove instance pointer
      out = null
    }

  }

  class ReportingOutputStream(val out: OutputStream, val message: String,
    val interval: FiniteDuration = 5 seconds, var size: Long = -1) extends CountingOutputStream(out) {
    override def write(buf: Array[Byte], start: Int, len: Int) {
      val append = if (size > 1) {
        s"/${Utils.readableFileSize(size, 2)} ${(100 * count / size).toInt}%"
      } else ""
      ProgressReporters.reporter.ephemeralMessage(s"$message ${Utils.readableFileSize(count)}$append")
      super.write(buf, start, len)
    }
  }

  class UnclosedFileOutputStream(f: File) extends FileOutputStream(f) {
    unclosedStreams += this
    override def close() {
      super.close()
      unclosedStreams -= this
    }

    override def equals(other: Any) = {
      other match {
        case x: UnclosedFileOutputStream => x eq this
        case _ => false
      }
    }

    override def hashCode() = {
      f.hashCode()
    }

  }

  val unclosedStreams = new HashSet[UnclosedFileOutputStream] with SynchronizedSet[UnclosedFileOutputStream]

  def closeAll() {
    l.info("Closing " + unclosedStreams.size + " outputstreams")
    while (!unclosedStreams.isEmpty)
      unclosedStreams.head.close
  }

}