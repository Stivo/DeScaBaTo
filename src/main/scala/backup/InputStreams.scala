package backup;

import java.io.InputStream
import java.io.OutputStream
import java.security.MessageDigest
import scala.collection.mutable.Buffer
import java.util.Arrays
import java.io.ByteArrayOutputStream
import java.io.ByteArrayInputStream

object Streams {
  
  class SplitInputStream(in: InputStream, outStreams: List[OutputStream]) extends InputStream {
    def read() = in.read()
    import ByteHandling.readFrom
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