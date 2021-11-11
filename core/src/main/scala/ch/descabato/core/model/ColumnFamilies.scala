package ch.descabato.core.model

import ch.descabato.CompressionMode
import ch.descabato.protobuf.keys.FileMetadataKey
import ch.descabato.protobuf.keys.FileMetadataValue
import ch.descabato.protobuf.keys.RevisionValue
import ch.descabato.protobuf.keys.ValueLogIndex
import ch.descabato.protobuf.keys.ValueLogStatusValue
import ch.descabato.utils.BytesWrapper
import ch.descabato.utils.CompressedStream
import ch.descabato.utils.Hash
import ch.descabato.utils.Implicits.ByteArrayUtils
import scalapb.GeneratedMessage

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

object ColumnFamilies {

  def lookupColumnFamily[K <: Key, V](key: Key): ColumnFamily[K, V] = {
    columnFamilies.filter(_.canHandle(key)).head.asInstanceOf[ColumnFamily[K, V]]
  }

  def decodeRevisionContentValue(y: RevisionContentValue): (Boolean, Key, Option[Product]) = {
    val cf = columnFamilies.find(_.ordinal == y.ordinal).get
    val key = cf.decodeKey(y.key)
    if (y.deletion) {
      (true, key, None)
    } else {
      (false, key, Some(cf.decodeValue(y.value.asArray())))
    }
  }

  val revisionContentColumnFamily = new RevisionContentColumnFamily()

  private val chunkColumnFamily = new ChunkColumnFamily()
  private val fileMetadataColumnFamily = new FileMetadataColumnFamily()
  private val fileStatusColumnFamily = new FileStatusColumnFamily()
  private val revisionColumnFamily = new RevisionColumnFamily()

  private val columnFamilies = Seq(
    chunkColumnFamily,
    fileMetadataColumnFamily,
    fileStatusColumnFamily,
    revisionColumnFamily,
    revisionContentColumnFamily,
  )
}

abstract class ColumnFamily[K <: Key, V] {

  def name: String

  def encodeKey(key: K): Array[Byte]

  def decodeKey(encoded: Array[Byte]): K

  def encodeValue(value: V): BytesWrapper

  def decodeValue(encoded: Array[Byte]): V

  def ordinal: Byte

  def isThisFamily(array: Array[Byte]): Boolean = array(0) == ordinal

  def canHandle(key: Key): Boolean

}

trait MessageValueColumnFamily[K <: Key, V <: GeneratedMessage] extends ColumnFamily[K, V] {
  override def encodeValue(value: V): BytesWrapper = {
    value.toByteArray.wrap()
  }
}

class RevisionColumnFamily extends ColumnFamily[Revision, RevisionValue] with MessageValueColumnFamily[Revision, RevisionValue] {
  override def name: String = "revision"

  override def canHandle(key: Key): Boolean = key match {
    case Revision(_) => true
    case _ => false
  }

  override def encodeKey(key: Revision): Array[Byte] = {
    Integer.toString(key.number).getBytes()
  }

  override def decodeKey(encoded: Array[Byte]): Revision = {
    Revision(new String(encoded).toInt)
  }

  override def decodeValue(encoded: Array[Byte]): RevisionValue = {
    RevisionValue.parseFrom(CompressedStream.decompressToBytes(encoded.wrap()).asArray())
  }

  override def encodeValue(value: RevisionValue): BytesWrapper = {
    CompressedStream.compress(super.encodeValue(value), CompressionMode.zstd9)
  }

  override def ordinal: Byte = 0
}

class FileMetadataColumnFamily extends ColumnFamily[FileMetadataKeyWrapper, FileMetadataValue] with MessageValueColumnFamily[FileMetadataKeyWrapper, FileMetadataValue] {
  override def name: String = "filemetadata"

  override def canHandle(key: Key): Boolean = key match {
    case FileMetadataKeyWrapper(_) => true
    case _ => false
  }

  override def encodeKey(key: FileMetadataKeyWrapper): Array[Byte] = {
    key.fileMetadataKey.toByteArray
  }

  override def decodeKey(encoded: Array[Byte]): FileMetadataKeyWrapper = {
    FileMetadataKeyWrapper(FileMetadataKey.parseFrom(encoded))
  }

  override def decodeValue(encoded: Array[Byte]): FileMetadataValue = {
    FileMetadataValue.parseFrom(encoded)
  }

  override def ordinal: Byte = 1
}

class ChunkColumnFamily extends ColumnFamily[ChunkKey, ValueLogIndex] with MessageValueColumnFamily[ChunkKey, ValueLogIndex] {
  override def name: String = "chunks"

  override def canHandle(key: Key): Boolean = key match {
    case ChunkKey(_) => true
    case _ => false
  }

  override def encodeKey(key: ChunkKey): Array[Byte] = {
    key.hash.bytes
  }

  override def decodeKey(encoded: Array[Byte]): ChunkKey = {
    ChunkKey(Hash(encoded))
  }

  override def decodeValue(encoded: Array[Byte]): ValueLogIndex = {
    ValueLogIndex.parseFrom(encoded)
  }

  override def ordinal: Byte = 3
}

class FileStatusColumnFamily extends ColumnFamily[ValueLogStatusKey, ValueLogStatusValue] with MessageValueColumnFamily[ValueLogStatusKey, ValueLogStatusValue] {
  override def name: String = "filestatus"

  override def canHandle(key: Key): Boolean = key match {
    case ValueLogStatusKey(_) => true
    case _ => false
  }

  override def encodeKey(key: ValueLogStatusKey): Array[Byte] = {
    key.name.getBytes(StandardCharsets.UTF_8)
  }

  override def decodeKey(encoded: Array[Byte]): ValueLogStatusKey = {
    ValueLogStatusKey(new String(encoded, StandardCharsets.UTF_8))
  }

  override def decodeValue(encoded: Array[Byte]): ValueLogStatusValue = {
    ValueLogStatusValue.parseFrom(encoded)
  }

  override def ordinal: Byte = 16
}

class RevisionContentColumnFamily extends ColumnFamily[RevisionContentKey, RevisionContentValue] {
  override def name: String = "revisionContent"

  override def canHandle(key: Key): Boolean = key match {
    case RevisionContentKey(_) => true
    case _ => false
  }

  override def encodeKey(key: RevisionContentKey): Array[Byte] = {
    val buffer = ByteBuffer.allocate(java.lang.Long.BYTES)
    buffer.putLong(key.number)
    buffer.array()
  }

  override def decodeKey(encoded: Array[Byte]): RevisionContentKey = {
    val buffer = ByteBuffer.allocate(java.lang.Long.BYTES)
    buffer.put(encoded)
    buffer.flip()
    val key = buffer.getLong
    RevisionContentKey(key)
  }

  override def decodeValue(encoded: Array[Byte]): RevisionContentValue = {
    RevisionContentValue.decode(encoded)
  }

  override def encodeValue(value: RevisionContentValue): BytesWrapper = {
    value.asArray()
  }

  override def ordinal: Byte = Byte.MaxValue
}
