package ch.descabato

import java.io.OutputStream
import java.io.InputStream
import scala.collection.mutable
import org.tukaani.xz.XZOutputStream
import java.util.zip.GZIPOutputStream
import org.tukaani.xz.LZMA2Options
import java.util.zip.GZIPInputStream
import org.tukaani.xz.XZInputStream
import java.io.File
import java.io.FileInputStream
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream

/**
 * Adds compressors and decompressors
 */
object CompressedStream extends Utils {
  
  def compress(content: Array[Byte], config: BackupFolderConfiguration, disableCompression: Boolean = false) : (Byte, Array[Byte]) = {
    val write = if (disableCompression) 0 else config.compressor.ordinal()
    if (write == 0) {
      return (0, content)
    }
    var baos = new ByteArrayOutputStream(content.length+16)
    val wrapped = getCompressor(write, baos)
    wrapped.write(content)
    wrapped.close()
    ObjectPools.byteArrayPool.recycle(content)
    val out = baos.toByteArray(true)
    (write.toByte, out)
  }

  def getCompressor(byte: Int, out: OutputStream) = {
    CompressionMode.getByByte(byte) match {
      case CompressionMode.gzip => new GZIPOutputStream(out)
      case CompressionMode.lzma => new XZOutputStream(out, new LZMA2Options())
      case CompressionMode.bzip2 => new BZip2CompressorOutputStream(out)
      case CompressionMode.none => out
    }
  }
  
  def readStream(in: InputStream): InputStream = {
    val byte = in.read()
    CompressionMode.getByByte(byte) match {
      case CompressionMode.gzip => new GZIPInputStream(in)
      case CompressionMode.lzma => new XZInputStream(in)
      case CompressionMode.bzip2 => new BZip2CompressorInputStream(in)
      case _ => in
    }
  }
  
  def wrapStream(out: OutputStream, config: BackupFolderConfiguration, disableCompression: Boolean = false) = {
    val write = if (disableCompression) 0 else config.compressor.getByte()
    out.write(write)
    getCompressor(write, out)
  }
}