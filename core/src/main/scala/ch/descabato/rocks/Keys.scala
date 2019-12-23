package ch.descabato.rocks

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.EOFException
import java.io.InputStream
import java.util
import java.util.Base64

import ch.descabato.rocks.protobuf.keys.FileMetadataKey

trait Key

case class ChunkKey(hash: Array[Byte]) extends Key {

  override def hashCode(): Int = {
    util.Arrays.hashCode(hash)
  }

  override def equals(obj: Any): Boolean = {
    obj match {
      case ChunkKey(other) =>
        util.Arrays.equals(hash, other)
      case _ =>
        false
    }

  }

  override def toString: String = s"Chunk(${Base64.getUrlEncoder.encode(hash)})"
}

case class Revision(number: Int) extends Key

case class RevisionContentKey(revision: Revision, number: Long) extends Key

case class ValueLogStatusKey(name: String) extends Key {
  def parseNumber: Int = {
    name.replaceAll("[^0-9]+", "").toInt
  }
}

case class FileMetadataKeyWrapper(fileMetadataKey: FileMetadataKey) extends Key

case class RevisionContentValue(ordinal: Byte, key: Array[Byte], value: Array[Byte]) {
  def asArray(withFullPrefix: Boolean): Array[Byte] = {
    val stream = new ByteArrayOutputStream(key.length + value.length + 20)
    val out = new DataOutputStream(stream)
    out.write(ordinal)
    if (withFullPrefix) {
      out.writeInt(key.length + value.length)
    }
    out.writeInt(key.length)
    out.write(key)
    out.writeInt(value.length)
    out.write(value)
    out.flush()
    stream.toByteArray
  }

}

object RevisionContentValue {
  def decode(encode: Array[Byte]): RevisionContentValue = {
    val stream = new ByteArrayInputStream(encode)
    readNextEntry(stream).get
  }

  def readNextEntry(stream: InputStream): Option[RevisionContentValue] = {
    var expectEof = true
    try {
      val stream1 = new DataInputStream(stream)
      val ordinal = stream1.readByte()
      expectEof = false
      val keyLength = stream1.readInt()
      val key = Array.ofDim[Byte](keyLength)
      stream1.readFully(key)
      val valueLength = stream1.readInt()
      val value = Array.ofDim[Byte](valueLength)
      stream1.readFully(value)
      Some(RevisionContentValue(ordinal, key, value))
    } catch {
      case _: EOFException if expectEof =>
        // could be the end of the stream, this would be valid
        None
    }
  }
}