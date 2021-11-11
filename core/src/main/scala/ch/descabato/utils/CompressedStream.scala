package ch.descabato.utils

import ch.descabato.CompressionMode
import ch.descabato.CustomByteArrayOutputStream
import ch.descabato.utils.Implicits._
import com.github.luben.zstd.ZstdInputStream
import com.github.luben.zstd.ZstdOutputStream
import net.jpountz.lz4.LZ4BlockInputStream
import net.jpountz.lz4.LZ4BlockOutputStream
import net.jpountz.lz4.LZ4Factory
import org.apache.commons.compress.utils.IOUtils
import org.iq80.snappy.SnappyFramedInputStream
import org.iq80.snappy.SnappyFramedOutputStream
import org.tukaani.xz.LZMA2Options
import org.tukaani.xz.XZInputStream
import org.tukaani.xz.XZOutputStream

import java.io.InputStream
import java.io.OutputStream
import java.util.zip._

/**
 * Adds compressors and decompressors
 */
object CompressedStream extends Utils {

  //  val timings = {
  //    var out = Map.empty[CompressionMode, StopWatch]
  //    out
  //  }

  def compress(content: BytesWrapper, compressor: CompressionMode): BytesWrapper = {
    val byte = compressor.getByte()
    val baos = new CustomByteArrayOutputStream(content.length + 80)
    baos.write(byte)
    val wrapped = getCompressor(byte, baos, Some(content.length))
    wrapped.write(content)
    wrapped.close()
    baos.toBytesWrapper()
  }

  def compressBytes(content: BytesWrapper, compressor: CompressionMode): CompressedBytes = {
    new CompressedBytes(compress(content, compressor), content.length)
  }

  private def roundUp(x: Int): Int = {
    var z = 4096
    while (z < x) {
      z = z * 2
    }
    z
  }

  def getCompressor(byte: Int, out: OutputStream, size: Option[Int] = None): OutputStream = {
    CompressionMode.getByByte(byte) match {
      case CompressionMode.gzip =>
        new GZIPOutputStream(out, 65536)
      case CompressionMode.`lzma6` =>
        val opt = new LZMA2Options()
        opt.setDictSize(1024 * 1024)
        size.foreach(x => opt.setDictSize(roundUp(x)))
        new XZOutputStream(out, opt)
      case c@(CompressionMode.`lzma1`
              | CompressionMode.`lzma3`)
      =>
        val opt = new LZMA2Options()
        opt.setPreset(c.name().dropWhile(!_.isDigit).toInt)
        opt.setDictSize(1024 * 1024)
        size.foreach(x => opt.setDictSize(roundUp(x)))
        new XZOutputStream(out, opt)
      case CompressionMode.snappy => new SnappyFramedOutputStream(out)
      case CompressionMode.deflate => new DeflaterOutputStream(out)
      case CompressionMode.lz4 => new LZ4BlockOutputStream(out, 1 << 12, LZ4Factory.fastestJavaInstance().fastCompressor())
      case CompressionMode.lz4hc => new LZ4BlockOutputStream(out, 1 << 12, LZ4Factory.fastestJavaInstance().highCompressor())
      case c@(CompressionMode.`zstd1` | CompressionMode.`zstd5` | CompressionMode.`zstd9`) =>
        val level = c.name().dropWhile(!_.isDigit).toInt
        new ZstdOutputStream(out, level)
      case CompressionMode.none => out
      case CompressionMode.smart => throw new IllegalArgumentException("Choose a concrete implementation")
    }
  }

  def decompress(input: BytesWrapper): InputStream = {
    val in = input.asInputStream()
    val byte = in.read()

    CompressionMode.getByByte(byte) match {
      case CompressionMode.gzip => new GZIPInputStream(in, 65536)
      case x if x.name().startsWith("lzma") => new XZInputStream(in)
      case x if x.name().startsWith("zstd") => new ZstdInputStream(in)
      case CompressionMode.snappy => new SnappyFramedInputStream(in, false)
      case CompressionMode.deflate => new InflaterInputStream(in)
      case CompressionMode.lz4 | CompressionMode.lz4hc => new LZ4BlockInputStream(in)
      case _ => in
    }
  }

  def decompressToBytes(input: BytesWrapper): BytesWrapper = {
    val stream = decompress(input)
    val baos = new CustomByteArrayOutputStream(input.length * 2)
    IOUtils.copy(stream, baos, 100 * 1024)
    IOUtils.closeQuietly(stream)
    IOUtils.closeQuietly(baos)
    baos.toBytesWrapper
  }

}

class CompressedBytes(val bytesWrapper: BytesWrapper, val uncompressedLength: Int)
