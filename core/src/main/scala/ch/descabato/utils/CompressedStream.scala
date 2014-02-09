package ch.descabato.utils

import java.io.OutputStream
import java.io.InputStream
import org.tukaani.xz.XZOutputStream
import java.util.zip.GZIPOutputStream
import org.tukaani.xz.LZMA2Options
import java.util.zip.GZIPInputStream
import org.tukaani.xz.XZInputStream
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream
import java.nio.ByteBuffer
import ch.descabato.ByteArrayOutputStream
import ch.descabato.CompressionMode

/**
 * Adds compressors and decompressors
 */
object CompressedStream extends Utils {
  
  def compress(content: Array[Byte], compressor: CompressionMode, disableCompression: Boolean = false) : (Byte, ByteBuffer) = {
    val write = if (disableCompression) 0 else compressor.getByte()
    if (write == 0) {
      return (0, ByteBuffer.wrap(content))
    }
    var baos = new ByteArrayOutputStream(content.length+16)
    val wrapped = getCompressor(write, baos)
    wrapped.write(content)
    wrapped.close()
    ObjectPools.byteArrayPool.recycle(content)
    val out = baos.toByteBuffer()
    (write.toByte, out)
  }

  def getCompressor(byte: Int, out: OutputStream) = {
    CompressionMode.getByByte(byte) match {
      case CompressionMode.gzip => new GZIPOutputStream(out, 65536)
      case CompressionMode.lzma => new XZOutputStream(out, new LZMA2Options())
      case CompressionMode.bzip2 => new BZip2CompressorOutputStream(out)
      case CompressionMode.none => out
    }
  }
  
  def readStream(in: InputStream): InputStream = {
    val byte = in.read()
    CompressionMode.getByByte(byte) match {
      case CompressionMode.gzip => new GZIPInputStream(in, 65536)
      case CompressionMode.lzma => new XZInputStream(in)
      case CompressionMode.bzip2 => new BZip2CompressorInputStream(in)
      case _ => in
    }
  }
  
  def wrapStream(out: OutputStream, compressor: CompressionMode, disableCompression: Boolean = false) = {
    val write = if (disableCompression) 0 else compressor.getByte()
    out.write(write)
    getCompressor(write, out)
  }
}