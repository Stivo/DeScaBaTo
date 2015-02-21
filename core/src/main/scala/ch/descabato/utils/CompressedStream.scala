package ch.descabato.utils

import java.io.{ByteArrayInputStream, InputStream, OutputStream}
import java.nio.ByteBuffer
import java.util.zip._

import ch.descabato.{ByteArrayOutputStream, CompressionMode}
import net.jpountz.lz4.{LZ4BlockInputStream, LZ4BlockOutputStream, LZ4Factory}
import org.apache.commons.compress.compressors.bzip2.{BZip2CompressorInputStream, BZip2CompressorOutputStream}
import org.iq80.snappy.{SnappyInputStream, SnappyOutputStream}
import org.tukaani.xz.{LZMA2Options, XZInputStream, XZOutputStream}

/**
 * Adds compressors and decompressors
 */
object CompressedStream extends Utils {
  
  def compress(content: Array[Byte], compressor: CompressionMode) : BytesWrapper = {
    val byte = compressor.getByte()
    val baos = new ByteArrayOutputStream(content.length+16)
    baos.write(byte)
    val wrapped = getCompressor(byte, baos, Some(content.length))
    wrapped.write(content)
    wrapped.close()
    baos.toBytesWrapper()
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
      case CompressionMode.smart => throw new IllegalArgumentException("Choose a concrete implementation")
    }
  }
  
  def decompress(input: Array[Byte]): InputStream = {
    val in = new ByteArrayInputStream(input)
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

  def decompressToBytes(input: Array[Byte]): BytesWrapper = {
    val stream = decompress(input)
    val baos = new ByteArrayOutputStream()
    Streams.copy(stream, baos)
    baos.toBytesWrapper
  }

}