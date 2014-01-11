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

object Streams {
  
  def readFully(in: InputStream)(implicit fileHandlingOptions: FileHandlingOptions) = {
    val baos = new ByteArrayOutputStream(10240)
    copy(wrapInputStream(in), baos)
    baos.toByteArray()
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
    var out = new ByteArrayOutputStream()
    val wrapped = wrapOutputStream(out)
    wrapped.write(content)
    wrapped.close()
    val r = out.toByteArray
    r
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
    
    var out = new ByteArrayOutputStream(blockSize+10)
    
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
    }
	  
  }
  
}