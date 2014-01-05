package backup;

import java.io.InputStream
import java.io.OutputStream
import java.security.MessageDigest
import scala.collection.mutable.Buffer
import java.util.Arrays
import java.io.ByteArrayOutputStream

object Streams {
  
  class SplitInputStream(in: InputStream, outStreams: List[OutputStream]) extends InputStream {
    def read() = in.read()
    def readComplete() {
      val buf = Array.ofDim[Byte](1024*1024)
      while (in.available() > 0) {
        val newOffset = in.read(buf, 0, buf.length)
        for (outStream <- outStreams) {
          outStream.write(buf, 0, newOffset)
        }
      }
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
  
  class BlockWritingOutputStream(val algorithm: String, val blockSize: Int) extends OutputStream {
    val md = MessageDigest.getInstance(algorithm)
    var cur = 0
    var result = Buffer[Array[Byte]]()
    def write(b : Int)  {
       md.update(b.toByte);
       cur += 1
       if (cur == blockSize) {
         finishBlock()
         cur = 0
       }
    }
  
    def finishBlock() {
      result += md.digest()
    }
    
    override def write(buf: Array[Byte], start: Int, len: Int)  {
      var (lenC, startC) = (len, start)
      while (lenC > 0) {
        val now = Math.min(blockSize-cur, lenC)
	    md.update(buf, startC, now);
        cur -= now
        lenC -= now
        if (cur == blockSize) {
          finishBlock()
          startC += lenC
        }
      }
    }
    
    override def close() {
      finishBlock()
      super.close()
    }

  }
  
}