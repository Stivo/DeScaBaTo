package ch.descabato.utils

import java.io.OutputStream
import java.io.InputStream
import org.tukaani.xz.{XZOutputStream, LZMA2Options, XZInputStream}
import java.util.zip._
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream
import java.nio.ByteBuffer
import ch.descabato.ByteArrayOutputStream
import ch.descabato.CompressionMode
import org.iq80.snappy.{SnappyInputStream, SnappyOutputStream}
import scala.Some
import org.apache.commons.compress.compressors.pack200.{Pack200CompressorOutputStream, Pack200CompressorInputStream}
import net.jpountz.lz4.{LZ4Compressor, LZ4Factory, LZ4BlockOutputStream, LZ4BlockInputStream}

/**
 * Adds compressors and decompressors
 */
object CompressedStream extends Utils {
  
  def compress(content: Array[Byte], compressor: CompressionMode) : (Byte, ByteBuffer) = {
    val write = compressor.getByte()
    if (write == 0) {
      return (0, ByteBuffer.wrap(content))
    }
    var baos = new ByteArrayOutputStream(content.length+16)
    val wrapped = getCompressor(write, baos, Some(content.length))
    wrapped.write(content)
    wrapped.close()
    val out = baos.toByteBuffer()
    (write.toByte, out)
  }

  def roundUp(x: Int) = {
    var z = 4096
    while (z < x) {
      z = z * 2
    }
    z
  }

  def getCompressor(byte: Int, out: OutputStream, size: Option[Int] = None) = {
    CompressionMode.getByByte(byte) match {
      case CompressionMode.gzip => new GZIPOutputStream(out, 65536)
      case CompressionMode.lzma =>
        val opt = new LZMA2Options()
        opt.setDictSize(1024*1024)
        size.foreach (x => opt.setDictSize(roundUp(x)))
        new XZOutputStream(out, opt)
      case CompressionMode.bzip2 =>
        val sizeBzip2 = BZip2CompressorOutputStream.chooseBlockSize(size.getOrElse(-1).toLong)
        new BZip2CompressorOutputStream(out, sizeBzip2)
      case CompressionMode.snappy => SnappyOutputStream.newChecksumFreeBenchmarkOutputStream(out)
      case CompressionMode.deflate => new DeflaterOutputStream(out)
      case CompressionMode.lz4 => new LZ4BlockOutputStream(out, 1<<12, LZ4Factory.fastestJavaInstance().fastCompressor())
      case CompressionMode.lz4hc => new LZ4BlockOutputStream(out, 1<<12, LZ4Factory.fastestJavaInstance().highCompressor())
      case CompressionMode.none => out
    }
  }
  
  def readStream(in: InputStream): InputStream = {
    val byte = in.read()

    CompressionMode.getByByte(byte) match {
      case CompressionMode.gzip => new GZIPInputStream(in, 65536)
      case CompressionMode.lzma => new XZInputStream(in)
      case CompressionMode.bzip2 => new BZip2CompressorInputStream(in)
      case CompressionMode.snappy => new SnappyInputStream(in, false)
      case CompressionMode.deflate => new InflaterInputStream(in)
      case CompressionMode.lz4 => new LZ4BlockInputStream(in)
      case CompressionMode.lz4hc => new LZ4BlockInputStream(in)
      case _ => in
    }
  }
  
  def wrapStream(out: OutputStream, compressor: CompressionMode) = {
    val write = compressor.getByte()
    out.write(write)
    getCompressor(write, out)
  }
}