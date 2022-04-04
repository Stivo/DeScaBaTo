package ch.descabato.core.model

import ch.descabato.CustomByteArrayOutputStream
import ch.descabato.utils.BytesWrapper
import ch.descabato.utils.Hash
import ch.descabato.utils.Implicits._
import com.google.protobuf.ByteString
import scalapb.TypeMapper

import java.io.ByteArrayInputStream
import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.EOFException
import java.io.InputStream
import java.util
import java.util.Objects


case class ChunkKey(hash: Hash) {

  override lazy val hashCode: Int = {
    hash.hashContent()
  }

  override def equals(obj: Any): Boolean = {
    obj match {
      case ChunkKey(other) =>
        hash === other
      case _ =>
        false
    }

  }

  override def toString: String = s"Chunk(${hash.toString})"
}

object ChunkKey {
  implicit val typeMapper: TypeMapper[ByteString, ChunkKey] =
    TypeMapper[ByteString, ChunkKey](x => ChunkKey(Hash(x.toByteArray)))(x => ByteString.copyFrom(x.hash.bytes))
}

case class RevisionKey(number: Int) extends AnyVal

object RevisionKey {
  implicit val typeMapper: TypeMapper[Int, RevisionKey] = TypeMapper(RevisionKey.apply)(_.number)
}


case class ValueLogStatusKey(name: String) extends AnyVal {
  def parseNumber: Int = {
    name.replaceAll("[^0-9]+", "").toInt
  }
}

object ValueLogStatusKey {
  implicit val typeMapper: TypeMapper[String, ValueLogStatusKey] = TypeMapper(ValueLogStatusKey.apply)(_.name)
}

case class RevisionContentValue private(ordinal: Byte, key: Array[Byte], value: BytesWrapper, deletion: Boolean) {
  def asArray(): BytesWrapper = {
    val stream = new CustomByteArrayOutputStream(key.length + value.length + 20)
    val out = new DataOutputStream(stream)
    val writeOrdinal: Int = if (deletion) ordinal - 128 else ordinal
    out.write(writeOrdinal)
    out.writeInt(key.length)
    out.write(key)
    if (!deletion) {
      out.writeInt(value.length)
      out.write(value)
    }
    out.flush()
    stream.toBytesWrapper
  }

  override def hashCode(): Int = {
    Objects.hash(util.Arrays.hashCode(key), value.hashCode)
  }

  override def equals(obj: Any): Boolean = {
    obj match {
      case x: RevisionContentValue =>
        Objects.equals(ordinal, x.ordinal) &&
          util.Arrays.equals(key, x.key) &&
          Objects.equals(value, x.value) &&
          Objects.equals(deletion, x.deletion)
      case _ => false
    }
  }
}

object RevisionContentValue {

  def createUpdate(ordinal: Byte, key: Array[Byte], value: BytesWrapper): RevisionContentValue = {
    RevisionContentValue(ordinal, key, value, false)
  }

  def createDelete(ordinal: Byte, key: Array[Byte]): RevisionContentValue = {
    RevisionContentValue(ordinal, key, BytesWrapper.empty, true)
  }

  def decode(encode: Array[Byte]): RevisionContentValue = {
    val stream = new ByteArrayInputStream(encode)
    readNextEntry(stream).get
  }

  def readNextEntry(stream: InputStream): Option[RevisionContentValue] = {
    var expectEof = true
    try {
      val stream1 = new DataInputStream(stream)
      var ordinal = stream1.readByte()
      val deletion = ordinal < 0
      if (deletion) {
        ordinal = (ordinal + 128).toByte
      }
      expectEof = false
      val keyLength = stream1.readInt()
      val key = Array.ofDim[Byte](keyLength)
      stream1.readFully(key)
      if (deletion) {
        Some(RevisionContentValue.createDelete(ordinal, key))
      } else {
        val valueLength = stream1.readInt()
        val value = Array.ofDim[Byte](valueLength)
        stream1.readFully(value)
        Some(RevisionContentValue.createUpdate(ordinal, key, value.wrap()))
      }
    } catch {
      case _: EOFException if expectEof =>
        // could be the end of the stream, this would be valid
        None
    }
  }
}
