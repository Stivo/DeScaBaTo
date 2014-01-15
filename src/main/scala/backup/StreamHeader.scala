package backup

import java.io.OutputStream
import java.io.InputStream
import scala.collection.mutable
import org.tukaani.xz.XZOutputStream
import com.jcraft.jzlib.GZIPOutputStream
import org.tukaani.xz.LZMA2Options
import java.util.zip.GZIPInputStream
import org.tukaani.xz.XZInputStream

/**
 * Wraps the stream in some way, encoding the options used so they can be decoded.
 */
trait StreamHeader {
  def wrapStream(out: OutputStream, options: FileHandlingOptions) : OutputStream
  
  def readStream(in: InputStream, passphrase: Option[String] = None) : InputStream
}

/**
 * Writes the version number into the stream. Then delegates to the correct
 * handlers.
 */
object StreamHeaders extends StreamHeader {
  
  private[backup] lazy val headers = mutable.Map[Int, StreamHeader]()
  lazy val init = {
    add(StreamHeaderV0)
    add(StreamHeaderV1)
    add(StreamHeaderV2)
    add(StreamHeaderV3)
    ()
  }
  
  private def add(x: StreamHeaderBase) {
    headers += x.version -> x
  }
  
  def wrapStream(out: OutputStream, options: FileHandlingOptions) = {
    init
    var version = 0
    if (options.passphrase != null) {
      version += 2
    }
    if (options.compression != CompressionMode.none) {
      version += 1
    }
    out.write(version)
	headers(version).wrapStream(out, options)
  }
  def readStream(in: InputStream, passphrase: Option[String] = None) : InputStream = {
    init
    val version = in.read()
    headers(version).readStream(in, passphrase)
  }
  
}

abstract class StreamHeaderBase extends StreamHeader {
  def version : Int
}

/**
 * Sequentally executes the given stream wrappers
 */
abstract class CompoundStreamHeader(args : StreamHeader*) extends StreamHeaderBase {
  def wrapStream(out: OutputStream, options: FileHandlingOptions) = {
    args.foldLeft(out)((out, streamHeader) => streamHeader.wrapStream(out, options))
  }
  
  def readStream(in: InputStream, passphrase: Option[String] = None) = {
    args.foldLeft(in)((in, streamHeader) => streamHeader.readStream(in, passphrase))
  }
}

object StreamHeaderV3 extends CompoundStreamHeader(StreamHeaderV1, StreamHeaderV2) {
  val version = 3
}

/**
 * Leaves the stream unchanged.
 */
object StreamHeaderV0 extends StreamHeaderBase {
  val version = 0
  def readStream(in: InputStream, passphrase: Option[String] = None) : InputStream = in
  def wrapStream(out: OutputStream, options: FileHandlingOptions) = out  
}

/**
 * Adds compressors and decompressors
 */
object StreamHeaderV1 extends StreamHeaderBase {
  val version = 1
  def readStream(in: InputStream, passphrase: Option[String] = None) : InputStream = {
    val byte = in.read()
    
    CompressionMode.values().apply(byte) match {
      case CompressionMode.gzip => new GZIPInputStream(in)
      case CompressionMode.lzma => new XZInputStream(in)
      case _ => in
    }

  }
  def wrapStream(out: OutputStream, options: FileHandlingOptions) = {
    out.write(options.compression.ordinal())
    options.compression match {
      case CompressionMode.gzip => new GZIPOutputStream(out)
      case CompressionMode.lzma => new XZOutputStream(out, new LZMA2Options())
      case none => out
    }
  }
}

/**
 * Adds AES encryption and decryption
 */
object StreamHeaderV2 extends StreamHeaderBase {
  val version = 2
  def readStream(in: InputStream, passphrase: Option[String] = None) : InputStream = {
    val byte = in.read()
    val enabled = (byte & (1 << 3)) != 0
    val keyLength = (byte >> 4) *64
    if (enabled && passphrase.isEmpty) {
      throw new IllegalArgumentException("Needs a password")
    }
    if (enabled)
      AES.wrapStreamWithDecryption(in, passphrase.get, keyLength)
    else
      in
  }
  def wrapStream(out: OutputStream, options: FileHandlingOptions) = {
   val keyLength = (options.keyLength / 64) << 4 // keep lower 4 bits for something else
   val encrypting = options.passphrase != null
   val enabled = if (encrypting) 1 << 3 else 0 // keep lower 3 bits for something else
   val desc = keyLength | enabled
   out.write(desc)
   if (options.passphrase != null) {
      if (options.algorithm == "AES") {
        AES.wrapStreamWithEncryption(out, options.passphrase, options.keyLength)
      } else {
        throw new IllegalArgumentException(s"Unknown encryption algorithm ${options.algorithm}")
      } 
    } else {
    	out
    }
  }
}