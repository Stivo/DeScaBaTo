package ch.descabato.remote

import java.io.{File, FileInputStream}

import ch.descabato.CustomByteArrayOutputStream
import ch.descabato.core.model.Size
import org.apache.commons.codec.digest.DigestUtils
import org.apache.commons.compress.utils.IOUtils

class S3EtagVerifier(file: File) {

  require(file.exists(), s"file $file must exist")

  def checkEtag(s: String): Boolean = {
    if (s.contains("-")) {
      val split = s.split("-", 2).toList
      val hex = split(0)
      val parts = split(1).toInt
      checkMultipart(hex, parts)
    } else {
      checkFullHash(s)
    }
  }

  def checkFullHash(hex: String): Boolean = {
    val stream = new FileInputStream(file)
    val computed = DigestUtils.md5Hex(stream)
    stream.close()
    computed == hex
  }

  def filterApplicapleSizes(sizes: Seq[Size], parts: Int) = {
    sizes.filter { size =>
      file.length() / size.bytes == parts - 1L ||
        file.length() / size.bytes == parts
    }
  }

  def checkMultipart(hex: String, parts: Int): Boolean = {
    val sizes = Seq(Size("5Mb"), Size("8Mb"), Size("16Mb"), Size("20Mb"))
    val fullETag = s"$hex-$parts"
    if (parts == 1) {
      computeEtag(sizes(0)) == fullETag
    } else {
      val possibleSizes = filterApplicapleSizes(sizes, parts)
      if (possibleSizes.size != 1) {
        throw new IllegalArgumentException(s"Could not find matching size for ${file.length()} bytes and $parts parts (found $possibleSizes)")
      }
      val str = computeEtag(possibleSizes(0))
      println(s"$file: Computed $str vs amazon $hex (partsize ${possibleSizes(0)})")
      str == fullETag
    }
  }

  def computeEtag(chunkSize: Size): String = {
    val stream = new FileInputStream(file)
    var filePos = 0L
    var parts = 0
    val output = new CustomByteArrayOutputStream()
    while (filePos < file.length()) {
      parts += 1
      val buffer = Array.ofDim[Byte](chunkSize.bytes.toInt)
      val i = IOUtils.readFully(stream, buffer)
      val bytes = DigestUtils.md5(buffer.slice(0, i))

      output.write(bytes)
      filePos += chunkSize.bytes
    }
    stream.close()
    DigestUtils.md5Hex(output.toBytesWrapper.asArray()) + "-" + parts
  }

}

