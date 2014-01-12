package backup;

import java.io.InputStream
import java.io.OutputStream
import java.security.MessageDigest
import scala.collection.mutable.Buffer
import java.util.Arrays
import java.io.ByteArrayOutputStream
import java.io.ByteArrayInputStream
import java.io.FileInputStream
import java.io.FileOutputStream
import java.util.zip.GZIPOutputStream
import java.util.zip.GZIPInputStream
import java.io.File
import scala.collection.mutable.Stack
import scala.ref.WeakReference
import scala.reflect.ClassTag
import scala.concurrent.duration._

object Streams {
  import ObjectPools.baosPool
  
  object ObjectPools {
    
    trait Arg[T<: AnyRef, A] {
      def isValidForArg(x: T, arg: A) : Boolean
    }
    
    trait NoneArg[T <: AnyRef] extends Arg[T, Unit] {
      self : ObjectPool[T, Unit] =>
        
      def isValidForArg(x: T, arg: Unit) = true
      def get() : T = get(())
      def makeNew() : T = get(())
    }
    
    trait ArrayArg[A] extends Arg[Array[A], Int] {
      self : ObjectPool[Array[A], Int] =>
      def classTag : ClassTag[A]
      def isValidForArg(x: Array[A], arg: Int) = x.length >= arg
      def makeNew(arg: Int) = Array.ofDim[A](arg)(classTag)
    }
   
    abstract class ObjectPool[T<: AnyRef, A] {
      self : Arg[T, A] =>
      private val stack = Buffer[WeakReference[T]]()
      def get(arg: A) : T = {
	      while (!stack.isEmpty) {
	        val toRemove = Buffer[WeakReference[T]]()
		    val result = stack.find(_ match { 
		          case wr@WeakReference(x) if (isValidForArg(x, arg))=> toRemove += wr; true
		          case WeakReference(x) => false // continue searching  
		          case wr => // This is an empty reference now, we might as well delete it 
		            toRemove += wr ; false
		    })
		     stack --= toRemove
		     result match {
	          	case Some(WeakReference(x)) => return x
	          	case _ => 
	        }
	      }
	      makeNew(arg)
      }
      def recycle(t: T) {
        reset(t)
        stack += (WeakReference(t))
      }
      protected def reset(t: T) {}
      protected def makeNew(arg: A) : T
    }

    val baosPool = new ObjectPool[ByteArrayOutputStream, Unit] with NoneArg[ByteArrayOutputStream] {
      override def reset(t: ByteArrayOutputStream) = t.reset()
      def makeNew(arg: Unit) = new ByteArrayOutputStream(1024*1024+10)
    }

    val byteArrayPool = new ObjectPool[Array[Byte], Int] with ArrayArg[Byte] {
      def classTag = ClassTag.Byte
    }
    
  }
  
  def readFully(in: InputStream)(implicit fileHandlingOptions: FileHandlingOptions) = {
    val baos = baosPool.get
    copy(wrapInputStream(in), baos)
    val out = baos.toByteArray()
    baosPool.recycle(baos)
    out
  }

  def newFileInputStream(file: File)(implicit fileHandlingOptions: FileHandlingOptions) = {
    var out: InputStream = new FileInputStream(file)
    wrapInputStream(out)
  }

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
    readFrom(in, { (x: Array[Byte], len: Int) =>
      out.write(x, 0, len)
    })
    in.close()
    out.close()
  }

  def wrapOutputStream(stream: OutputStream)(implicit fileHandlingOptions: FileHandlingOptions) : OutputStream = {
    var out = stream
    if (fileHandlingOptions.passphrase != null) {
      if (fileHandlingOptions.algorithm == "AES") {
        out = AES.wrapStreamWithEncryption(out, fileHandlingOptions.passphrase, fileHandlingOptions.keyLength)
      } else {
        throw new IllegalArgumentException(s"Unknown encryption algorithm ${fileHandlingOptions.algorithm}")
      }
    }
    if (fileHandlingOptions.compression == CompressionMode.zip) {
      out = new GZIPOutputStream(out)
    }
    out
  }
  
  def newFileOutputStream(file: File)(implicit fileHandlingOptions: FileHandlingOptions) : OutputStream = {
    var out: OutputStream = new FileOutputStream(file)
    wrapOutputStream(out)
  }

  def newByteArrayOut(content: Array[Byte])(implicit fileHandlingOptions: FileHandlingOptions) = {
    var baos = baosPool.get
    val wrapped = wrapOutputStream(baos)
    wrapped.write(content)
    wrapped.close()
    val out = baos.toByteArray
    baosPool.recycle(baos)
    out
  }
  

  def wrapInputStream(in: InputStream)(implicit fileHandlingOptions: FileHandlingOptions) = {
    var out = in
    if (fileHandlingOptions.passphrase != null) {
      if (fileHandlingOptions.algorithm == "AES") {
        out = AES.wrapStreamWithDecryption(out, fileHandlingOptions.passphrase, fileHandlingOptions.keyLength)
      } else {
        throw new IllegalArgumentException(s"Unknown encryption algorithm ${fileHandlingOptions.algorithm}")
      }
    }
    if (fileHandlingOptions.compression == CompressionMode.zip) {
      out = new GZIPInputStream(out)
    }
    out
  }

  
  class SplitInputStream(in: InputStream, outStreams: List[OutputStream]) extends InputStream {
    def read() = in.read()
    def readComplete() {
      readFrom(in, {(buf: Array[Byte], len: Int) =>
        for (outStream <- outStreams) {
          outStream.write(buf, 0, len)
        }
      })
      outStreams.foreach(_.close)
    }
  }
  
  class HashingOutputStream(val algorithm: String) extends OutputStream {
    val md = MessageDigest.getInstance(algorithm)
    
    var out: Option[Array[Byte]] = None
    
    def write(b : Int)  {
       md.update(b.toByte);
    }
  
    override def write(buf: Array[Byte], start: Int, len: Int)  {
      md.update(buf, start, len);
    }

    override def close() {
      out = Some(md.digest())
      super.close()
    }
    
  }
  
  class CountingOutputStream(val stream: OutputStream) extends OutputStream {
    var counter: Long = 0
    def write(b : Int)  {
      counter += 1
      stream.write(b)
    }
  
    override def write(buf: Array[Byte], start: Int, len: Int)  {
      counter += len
      stream.write(buf, start, len)
    }

    def count() = counter
    
    override def close() = stream.close()
  }

  class BlockOutputStream(val blockSize: Int, func: (Array[Byte] => _)) extends OutputStream {
    
    var out = baosPool.get
    
    def write(b : Int)  {
      out.write(b)
      handleEnd()
    }
  
    def handleEnd() {
      if (cur == blockSize) {
    	  func(out.toByteArray())
    	  out.reset()
      }
    }
    
    def cur = out.size()
    
    override def write(buf: Array[Byte], start: Int, len: Int)  {
      var (lenC, startC) = (len, start)
      while (lenC > 0) {
        val now = Math.min(blockSize-cur, lenC)
	    out.write(buf, startC, now)
        lenC -= now
        startC += now
        handleEnd()
      }
    }
    
    override def close() {
      func(out.toByteArray())
      super.close()
      baosPool.recycle(out)
      // out was returned to the pool, so remove instance pointer
      out = null
    }
	  
  }
  
  class ReportingOutputStream(val out: OutputStream, val message: String, 
		  val interval: FiniteDuration = 5 seconds, var size : Long = -1) extends CountingOutputStream(out) {
	override def write(buf: Array[Byte], start: Int, len: Int)  {
	  val append = if (size > 1) {
	    s"/${Utils.readableFileSize(size, 2)} ${(100*count/size).toInt}%"
	  } else ""
	  ConsoleManager.writeDeleteLine(s"$message ${Utils.readableFileSize(count)}$append")
	  super.write(buf, start, len)
	}
  }
  
}