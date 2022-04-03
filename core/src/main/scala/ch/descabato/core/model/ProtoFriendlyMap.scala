package ch.descabato.core.model

import ch.descabato.protobuf.keys.ChunkProtoMap
import ch.descabato.protobuf.keys.FileMetadataKey
import ch.descabato.protobuf.keys.FileMetadataProtoMap
import ch.descabato.protobuf.keys.FileMetadataValue
import ch.descabato.protobuf.keys.ValueLogIndex
import ch.descabato.utils.Hash
import scalapb.TypeMapper

import scala.collection.immutable.HashMap


trait IntKey[T] {
  def id(t: T): Int

  def newInstance(id: Int): T

  def increment(t: T): T = newInstance(id(t) + 1)

  def max(t1: T, t2: T): T = if (id(t1) > id(t2)) t1 else t2
}

/**
 * A class that can be easily serialized for proto with very little overhead when reading / writing.
 * Not threadsafe.
 *
 * @tparam Key      The key type of the actual entity
 * @tparam KeyId    The internal type used as a primary key for serialized entities
 * @tparam Value    The value type of the actual entity
 * @tparam ProtoMap The type of the proto map
 */
abstract class ProtoFriendlyMap[Key, KeyId, Value, ProtoMap](private var _keyMap: HashMap[Key, KeyId], private var _valueMap: HashMap[KeyId, Value]) {

  protected def idCompanion: IntKey[KeyId]

  private var _highestKeyId: KeyId = computeHighestKeyId()

  private def computeHighestKeyId(): KeyId = {
    _valueMap.keys.foldLeft(idCompanion.newInstance(0))(idCompanion.max)
  }

  private var _updatesKeyMap: HashMap[KeyId, Key] = HashMap.empty
  private var _updatesValueMap: HashMap[KeyId, Value] = HashMap.empty

  def keyMap: HashMap[Key, KeyId] = _keyMap

  def valueMap: HashMap[KeyId, Value] = _valueMap

  def updatesKeyMap: HashMap[KeyId, Key] = _updatesKeyMap

  def updatesValueMap: HashMap[KeyId, Value] = _updatesValueMap

  def add(k: Key, v: Value): KeyId = {
    val newId = if (_keyMap.contains(k)) {
      val keyId = _keyMap(k)
      _valueMap += keyId -> v
      keyId
    } else {
      val newId = idCompanion.increment(_highestKeyId)
      _keyMap += k -> newId
      _valueMap += newId -> v
      _highestKeyId = newId
      newId
    }
    _updatesKeyMap += newId -> k
    _updatesValueMap += newId -> v
    newId
  }

  def getByKey(k: Key): Option[(KeyId, Value)] = {
    for {
      keyId <- _keyMap.get(k)
      value <- _valueMap.get(keyId)
    } {
      return Some((keyId, value))
    }
    None
  }

  def mergeWithOther(other: ProtoFriendlyMap[Key, KeyId, Value, ProtoMap]): Unit = {
    _keyMap ++= other._keyMap
    _valueMap ++= other._valueMap
    _highestKeyId = computeHighestKeyId()
  }

  def getByKeyId(k: KeyId): Option[Value] = {
    _valueMap.get(k)
  }

  def getProtoKeyMap(protoMap: ProtoMap): HashMap[KeyId, Key]

  def getProtoValueMap(protoMap: ProtoMap): HashMap[KeyId, Value]

  def exportForProto(): ProtoMap

}

object ProtoFriendlyMap {
  def reverseMap[K, V](map: HashMap[K, V]): HashMap[V, K] = {
    map.map { case (k, v) => v -> k }
  }

}

case class ChunkMapKeyId(id: Int) extends AnyVal {
  def increment(): ChunkMapKeyId = {
    ChunkMapKeyId(id + 1)
  }
}

object ChunkMapKeyId extends IntKey[ChunkMapKeyId] {
  implicit val typeMapper: TypeMapper[Int, ChunkMapKeyId] = TypeMapper(ChunkMapKeyId.apply)(_.id)

  def addOne(key: ChunkMapKeyId): ChunkMapKeyId = ChunkMapKeyId(key.id + 1)

  //  def max(key1: ChunkMapKeyId, key2: ChunkMapKeyId): ChunkMapKeyId = ChunkMapKeyId(Math.max(key1.id, key2.id))

  override def id(t: ChunkMapKeyId): Int = t.id

  override def newInstance(id: Int): ChunkMapKeyId = ChunkMapKeyId(id)
}

class ChunkMap private(keyMap: HashMap[Hash, ChunkMapKeyId], valueMap: HashMap[ChunkMapKeyId, ValueLogIndex]) extends ProtoFriendlyMap[Hash, ChunkMapKeyId, ValueLogIndex, ChunkProtoMap](keyMap, valueMap) {

  override protected lazy val idCompanion: IntKey[ChunkMapKeyId] = ChunkMapKeyId

  override def exportForProto(): ChunkProtoMap = {
    ChunkProtoMap(updatesKeyMap, updatesValueMap)
  }

  override def getProtoKeyMap(protoMap: ChunkProtoMap): HashMap[ChunkMapKeyId, Hash] = protoMap.chunkKeys

  override def getProtoValueMap(protoMap: ChunkProtoMap): HashMap[ChunkMapKeyId, ValueLogIndex] = protoMap.chunkValues

}

object ChunkMap {
  def importFromProto(protoMap: ChunkProtoMap): ChunkMap = {
    new ChunkMap(ProtoFriendlyMap.reverseMap(protoMap.chunkKeys), protoMap.chunkValues)
  }

  def empty: ChunkMap = {
    new ChunkMap(HashMap.empty, HashMap.empty)
  }
}

case class FileMetadataKeyId(id: Int) extends AnyVal {
  def increment(): FileMetadataKeyId = {
    FileMetadataKeyId(id + 1)
  }
}

object FileMetadataKeyId extends IntKey[FileMetadataKeyId] {
  implicit val typeMapper: TypeMapper[Int, FileMetadataKeyId] = TypeMapper(FileMetadataKeyId.apply)(_.id)

  def addOne(key: FileMetadataKeyId): FileMetadataKeyId = FileMetadataKeyId(key.id + 1)

  override def id(t: FileMetadataKeyId): Int = t.id

  override def newInstance(id: Int): FileMetadataKeyId = FileMetadataKeyId(id)
}

class FileMetadataMap private(keyMap: HashMap[FileMetadataKey, FileMetadataKeyId], valueMap: HashMap[FileMetadataKeyId, FileMetadataValue])
  extends ProtoFriendlyMap[FileMetadataKey, FileMetadataKeyId, FileMetadataValue, FileMetadataProtoMap](keyMap, valueMap) {

  override protected lazy val idCompanion: IntKey[FileMetadataKeyId] = FileMetadataKeyId

  override def exportForProto(): FileMetadataProtoMap = {
    FileMetadataProtoMap(updatesKeyMap, updatesValueMap)
  }

  override def getProtoKeyMap(protoMap: FileMetadataProtoMap): HashMap[FileMetadataKeyId, FileMetadataKey] = protoMap.fileMetadataKeys

  override def getProtoValueMap(protoMap: FileMetadataProtoMap): HashMap[FileMetadataKeyId, FileMetadataValue] = protoMap.fileMetadataValues

}

object FileMetadataMap {
  def importFromProto(protoMap: FileMetadataProtoMap): FileMetadataMap = {
    new FileMetadataMap(ProtoFriendlyMap.reverseMap(protoMap.fileMetadataKeys), protoMap.fileMetadataValues)
  }

  def empty: FileMetadataMap = {
    new FileMetadataMap(HashMap.empty, HashMap.empty)
  }
}