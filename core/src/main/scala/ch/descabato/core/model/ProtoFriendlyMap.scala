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
 * @tparam Id    The internal type used as a primary key for serialized entities
 * @tparam Value The value type of the actual entity
 */
abstract class ProtoFriendlyMap[Key, Id, Value, MapType](private var _keyMap: HashMap[Key, Id], private var _idMap: HashMap[Id, (Key, Value)]) {

  protected def idCompanion: IntKey[Id]

  private var _highestKeyId: Id = computeHighestKeyId()

  private def computeHighestKeyId(): Id = {
    _idMap.keys.foldLeft(idCompanion.newInstance(0))(idCompanion.max)
  }

  def keyMap: HashMap[Key, Id] = _keyMap

  def valueMap: HashMap[Id, (Key, Value)] = _idMap

  def add(k: Key, v: Value): Id = {
    val newId = if (_keyMap.contains(k)) {
      val keyId = _keyMap(k)
      _idMap += keyId -> (k, v)
      keyId
    } else {
      val newId = idCompanion.increment(_highestKeyId)
      _keyMap += k -> newId
      _idMap += newId -> (k, v)
      _highestKeyId = newId
      newId
    }
    newId
  }

  def getByKey(k: Key): Option[(Id, Value)] = {
    for {
      keyId <- _keyMap.get(k)
      value <- _idMap.get(keyId).map(_._2)
    } {
      return Some((keyId, value))
    }
    None
  }

  def getById(k: Id): Option[(Key, Value)] = {
    _idMap.get(k)
  }

  def iterator(): Iterator[(Key, Value)] = _idMap.valuesIterator

  protected def constructNew(newKeyMap: HashMap[Key, Id], newKeyIdMap: HashMap[Id, (Key, Value)]): MapType

  def merge(other: ProtoFriendlyMap[Key, Id, Value, MapType]): MapType = {
    val newKeyMap = _keyMap ++ other._keyMap
    val newIdMap = _idMap ++ other._idMap
    constructNew(newKeyMap, newIdMap)
  }

}

object ProtoFriendlyMap {
  def reverseMap[K, V](map: HashMap[K, V]): HashMap[V, K] = {
    map.map { case (k, v) => v -> k }
  }
}

case class ChunkId(id: Int) extends AnyVal

object ChunkId extends IntKey[ChunkId] {
  implicit val typeMapper: TypeMapper[Int, ChunkId] = TypeMapper(ChunkId.apply)(_.id)

  override def id(t: ChunkId): Int = t.id

  override def newInstance(id: Int): ChunkId = ChunkId(id)
}

class ChunkMap private(keyMap: HashMap[ChunkKey, ChunkId], valueMap: HashMap[ChunkId, (ChunkKey, ValueLogIndex)])
  extends ProtoFriendlyMap[ChunkKey, ChunkId, ValueLogIndex, ChunkMap](keyMap, valueMap) {

  override protected lazy val idCompanion: IntKey[ChunkId] = ChunkId

  override def constructNew(newKeyMap: HashMap[ChunkKey, ChunkId], newIdMap: HashMap[ChunkId, (ChunkKey, ValueLogIndex)]): ChunkMap =
    new ChunkMap(newKeyMap, newIdMap)
}

object ChunkMap {
  def importFromProto(protoMap: ChunkProtoMap): ChunkMap = {
    val reversedKeys = ProtoFriendlyMap.reverseMap(protoMap.chunkKeys)
    val values = protoMap.chunkValues.map { case (id, value) =>
      id -> (protoMap.chunkKeys(id), value)
    }
    new ChunkMap(reversedKeys, values)
  }

  def empty: ChunkMap = {
    new ChunkMap(HashMap.empty, HashMap.empty)
  }
}

case class FileMetadataId(id: Int) extends AnyVal

object FileMetadataId extends IntKey[FileMetadataId] {
  implicit val typeMapper: TypeMapper[Int, FileMetadataId] = TypeMapper(FileMetadataId.apply)(_.id)

  def addOne(key: FileMetadataId): FileMetadataId = FileMetadataId(key.id + 1)

  override def id(t: FileMetadataId): Int = t.id

  override def newInstance(id: Int): FileMetadataId = FileMetadataId(id)
}

class FileMetadataMap private(keyMap: HashMap[FileMetadataKey, FileMetadataId], valueMap: HashMap[FileMetadataId, (FileMetadataKey, FileMetadataValue)])
  extends ProtoFriendlyMap[FileMetadataKey, FileMetadataId, FileMetadataValue, FileMetadataMap](keyMap, valueMap) {

  override def constructNew(newKeyMap: HashMap[FileMetadataKey, FileMetadataId], newIdMap: HashMap[FileMetadataId, (FileMetadataKey, FileMetadataValue)]): FileMetadataMap =
    new FileMetadataMap(newKeyMap, newIdMap)

  override protected lazy val idCompanion: IntKey[FileMetadataId] = FileMetadataId

}

object FileMetadataMap {
  def importFromProto(protoMap: FileMetadataProtoMap): FileMetadataMap = {
    val keyMap = ProtoFriendlyMap.reverseMap(protoMap.fileMetadataKeys)
    val values = protoMap.fileMetadataValues.map { case (id, value) =>
      id -> (protoMap.fileMetadataKeys(id), value)
    }
    new FileMetadataMap(keyMap, values)
  }

  def empty: FileMetadataMap = {
    new FileMetadataMap(HashMap.empty, HashMap.empty)
  }
}