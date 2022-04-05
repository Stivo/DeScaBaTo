package ch.descabato.core.model

import ch.descabato.protobuf.keys.ChunkProtoMap
import ch.descabato.protobuf.keys.FileMetadataKey
import ch.descabato.protobuf.keys.FileMetadataProtoMap
import ch.descabato.protobuf.keys.FileMetadataValue
import ch.descabato.protobuf.keys.ValueLogIndex
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
 * @tparam Key   The key type of the actual entity
 * @tparam KeyId The internal type used as a primary key for serialized entities
 * @tparam Value The value type of the actual entity
 */
abstract class ProtoFriendlyMap[Key, KeyId, Value, MapType](private var _keyMap: HashMap[Key, KeyId], private var _keyIdMap: HashMap[KeyId, (Key, Value)]) {

  protected def idCompanion: IntKey[KeyId]

  private var _highestKeyId: KeyId = computeHighestKeyId()

  private def computeHighestKeyId(): KeyId = {
    _keyIdMap.keys.foldLeft(idCompanion.newInstance(0))(idCompanion.max)
  }

  def keyMap: HashMap[Key, KeyId] = _keyMap

  def valueMap: HashMap[KeyId, (Key, Value)] = _keyIdMap

  def add(k: Key, v: Value): KeyId = {
    val newId = if (_keyMap.contains(k)) {
      val keyId = _keyMap(k)
      _keyIdMap += keyId -> (k, v)
      keyId
    } else {
      val newId = idCompanion.increment(_highestKeyId)
      _keyMap += k -> newId
      _keyIdMap += newId -> (k, v)
      _highestKeyId = newId
      newId
    }
    newId
  }

  def getByKey(k: Key): Option[(KeyId, Value)] = {
    for {
      keyId <- _keyMap.get(k)
      value <- _keyIdMap.get(keyId).map(_._2)
    } {
      return Some((keyId, value))
    }
    None
  }

  def getByKeyId(k: KeyId): Option[(Key, Value)] = {
    _keyIdMap.get(k)
  }

  def iterator(): Iterator[(Key, Value)] = _keyIdMap.valuesIterator

  protected def constructNew(newKeyMap: HashMap[Key, KeyId], newKeyIdMap: HashMap[KeyId, (Key, Value)]): MapType

  def merge(other: ProtoFriendlyMap[Key, KeyId, Value, MapType]): MapType = {
    val newKeyMap = _keyMap ++ other._keyMap
    val newKeyIdMap = _keyIdMap ++ other._keyIdMap
    constructNew(newKeyMap, newKeyIdMap)
  }

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

  override def id(t: ChunkMapKeyId): Int = t.id

  override def newInstance(id: Int): ChunkMapKeyId = ChunkMapKeyId(id)
}

class ChunkMap private(keyMap: HashMap[ChunkKey, ChunkMapKeyId], valueMap: HashMap[ChunkMapKeyId, (ChunkKey, ValueLogIndex)])
  extends ProtoFriendlyMap[ChunkKey, ChunkMapKeyId, ValueLogIndex, ChunkMap](keyMap, valueMap) {

  override protected lazy val idCompanion: IntKey[ChunkMapKeyId] = ChunkMapKeyId

  override def constructNew(newKeyMap: HashMap[ChunkKey, ChunkMapKeyId], newKeyIdMap: HashMap[ChunkMapKeyId, (ChunkKey, ValueLogIndex)]): ChunkMap =
    new ChunkMap(newKeyMap, newKeyIdMap)
}

object ChunkMap {
  def importFromProto(protoMap: ChunkProtoMap): ChunkMap = {
    val reversedKeys = ProtoFriendlyMap.reverseMap(protoMap.chunkKeys)
    val values = protoMap.chunkValues.map { case (keyId, value) =>
      keyId -> (protoMap.chunkKeys(keyId), value)
    }
    new ChunkMap(reversedKeys, values)
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

class FileMetadataMap private(keyMap: HashMap[FileMetadataKey, FileMetadataKeyId], valueMap: HashMap[FileMetadataKeyId, (FileMetadataKey, FileMetadataValue)])
  extends ProtoFriendlyMap[FileMetadataKey, FileMetadataKeyId, FileMetadataValue, FileMetadataMap](keyMap, valueMap) {

  override def constructNew(newKeyMap: HashMap[FileMetadataKey, FileMetadataKeyId], newKeyIdMap: HashMap[FileMetadataKeyId, (FileMetadataKey, FileMetadataValue)]): FileMetadataMap =
    new FileMetadataMap(newKeyMap, newKeyIdMap)

  override protected lazy val idCompanion: IntKey[FileMetadataKeyId] = FileMetadataKeyId

}

object FileMetadataMap {
  def importFromProto(protoMap: FileMetadataProtoMap): FileMetadataMap = {
    val keyMap = ProtoFriendlyMap.reverseMap(protoMap.fileMetadataKeys)
    val values = protoMap.fileMetadataValues.map { case (keyId, value) =>
      keyId -> (protoMap.fileMetadataKeys(keyId), value)
    }
    new FileMetadataMap(keyMap, values)

  }

  def empty: FileMetadataMap = {
    new FileMetadataMap(HashMap.empty, HashMap.empty)
  }
}