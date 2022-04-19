package ch.descabato.core.model

import ch.descabato.core.FastMap
import ch.descabato.protobuf.keys.ChunkProtoMap
import ch.descabato.protobuf.keys.FileMetadataKey
import ch.descabato.protobuf.keys.FileMetadataProtoMap
import ch.descabato.protobuf.keys.FileMetadataValue
import ch.descabato.protobuf.keys.ValueLogIndex
import ch.descabato.utils.Hash
import scalapb.TypeMapper

import scala.collection.immutable.HashMap
import scala.collection.mutable


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
abstract class ProtoFriendlyMap[Key, Id, Value, MapType](private var _keyMap: mutable.Map[Key, Id], private var _idMap: mutable.Map[Id, (Key, Value)]) {

  protected def idCompanion: IntKey[Id]

  private var _highestKeyId: Id = computeHighestKeyId()

  private def computeHighestKeyId(): Id = {
    _idMap.keys.foldLeft(idCompanion.newInstance(0))(idCompanion.max)
  }

  def keyMap: mutable.Map[Key, Id] = _keyMap

  def valueMap: mutable.Map[Id, (Key, Value)] = _idMap

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

  protected def constructNew(newKeyMap: mutable.Map[Key, Id], newKeyIdMap: mutable.Map[Id, (Key, Value)]): MapType

  def merge(other: ProtoFriendlyMap[Key, Id, Value, MapType]): MapType = {
    val newKeyMap = _keyMap ++ other._keyMap
    val newIdMap = _idMap ++ other._idMap
    constructNew(newKeyMap, newIdMap)
  }

}

object ProtoFriendlyMap {
  def reverseMap[K, V](map: HashMap[K, V]): mutable.Map[V, K] = {
    val out = new mutable.HashMap[V, K]()
    map.foreach { case (k, v) => (v, k) }
    out
  }
}

case class ChunkId(id: Int) extends AnyVal

object ChunkId extends IntKey[ChunkId] {
  implicit val typeMapper: TypeMapper[Int, ChunkId] = TypeMapper(ChunkId.apply)(_.id)

  override def id(t: ChunkId): Int = t.id

  override def newInstance(id: Int): ChunkId = ChunkId(id)
}

class ChunkMap private(keyMap: FastMap[ChunkId], valueMap: mutable.Map[ChunkId, (Hash, ValueLogIndex)])
  extends ProtoFriendlyMap[Hash, ChunkId, ValueLogIndex, ChunkMap](keyMap, valueMap) {

  override protected lazy val idCompanion: IntKey[ChunkId] = ChunkId

  override def constructNew(newKeyMap: mutable.Map[Hash, ChunkId], newIdMap: mutable.Map[ChunkId, (Hash, ValueLogIndex)]): ChunkMap =
    new ChunkMap(newKeyMap.asInstanceOf[FastMap[ChunkId]], newIdMap)

  override def merge(other: ProtoFriendlyMap[Hash, ChunkId, ValueLogIndex, ChunkMap]): ChunkMap = {
    val newKeyMap = FastMap[ChunkId]
    newKeyMap ++= keyMap
    newKeyMap ++= other.keyMap
    val newIdMap = valueMap ++ other.valueMap
    constructNew(newKeyMap, newIdMap)
  }

}

object ChunkMap {
  def importFromProto(protoMap: ChunkProtoMap): ChunkMap = {
    val reversedKeys = new FastMap[ChunkId]()
    protoMap.chunkKeys.foreach { case (k, v) => (v.hash, k) }

    val valuesMap = new mutable.HashMap[ChunkId, (Hash, ValueLogIndex)]()
    protoMap.chunkValues.foreach { case (id, value) =>
      valuesMap += id -> (protoMap.chunkKeys(id).hash, value)
    }
    new ChunkMap(reversedKeys, valuesMap)
  }

  def empty: ChunkMap = {
    new ChunkMap(FastMap.empty, mutable.Map.empty)
  }
}

case class FileMetadataId(id: Int) extends AnyVal

object FileMetadataId extends IntKey[FileMetadataId] {
  implicit val typeMapper: TypeMapper[Int, FileMetadataId] = TypeMapper(FileMetadataId.apply)(_.id)

  def addOne(key: FileMetadataId): FileMetadataId = FileMetadataId(key.id + 1)

  override def id(t: FileMetadataId): Int = t.id

  override def newInstance(id: Int): FileMetadataId = FileMetadataId(id)
}

class FileMetadataMap private(keyMap: mutable.Map[FileMetadataKey, FileMetadataId], valueMap: mutable.Map[FileMetadataId, (FileMetadataKey, FileMetadataValue)])
  extends ProtoFriendlyMap[FileMetadataKey, FileMetadataId, FileMetadataValue, FileMetadataMap](keyMap, valueMap) {

  override def constructNew(newKeyMap: mutable.Map[FileMetadataKey, FileMetadataId], newIdMap: mutable.Map[FileMetadataId, (FileMetadataKey, FileMetadataValue)]): FileMetadataMap =
    new FileMetadataMap(newKeyMap, newIdMap)

  override protected lazy val idCompanion: IntKey[FileMetadataId] = FileMetadataId

}

object FileMetadataMap {
  def importFromProto(protoMap: FileMetadataProtoMap): FileMetadataMap = {
    val keyMap = ProtoFriendlyMap.reverseMap(protoMap.fileMetadataKeys)
    val valueMap = mutable.HashMap.empty[FileMetadataId, (FileMetadataKey, FileMetadataValue)]
    protoMap.fileMetadataValues.foreach { case (id, value) =>
      valueMap += id -> (protoMap.fileMetadataKeys(id), value)
    }
    new FileMetadataMap(keyMap, valueMap)
  }

  def empty: FileMetadataMap = {
    new FileMetadataMap(mutable.HashMap.empty, mutable.Map.empty)
  }
}