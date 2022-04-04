package ch.descabato.core.util

import ch.descabato.core.model.ChunkKey
import ch.descabato.core.model.RevisionKey
import ch.descabato.core.model.ValueLogStatusKey
import ch.descabato.protobuf.keys.FileMetadataKey
import ch.descabato.protobuf.keys.FileMetadataValue
import ch.descabato.protobuf.keys.RevisionValue
import ch.descabato.protobuf.keys.ValueLogIndex
import ch.descabato.protobuf.keys.ValueLogStatusValue

class InMemoryDbOld {

  private var _fileMetadataKeyRepository = Map.empty[FileMetadataKey, FileMetadataKey]

  private var _fileMetadata = Map.empty[FileMetadataKey, FileMetadataValue]
  private var _valueLogStatus = Map.empty[ValueLogStatusKey, ValueLogStatusValue]
  private var _revision = Map.empty[RevisionKey, RevisionValue]

  def chunks: Map[ChunkKey, ValueLogIndex] = ???

  def fileMetadata: Map[FileMetadataKey, FileMetadataValue] = _fileMetadata

  def valueLogStatus: Map[ValueLogStatusKey, ValueLogStatusValue] = _valueLogStatus

  def revision: Map[RevisionKey, RevisionValue] = _revision

  def readChunk(chunkKey: ChunkKey): Option[ValueLogIndex] = ???

  def readFileMetadata(fileMetadata: FileMetadataKey): Option[FileMetadataValue] = ???

  def readRevision(revision: RevisionKey): Option[RevisionValue] = _revision.get(revision)

  def readValueLogStatus(valueLogStatusKey: ValueLogStatusKey): Option[ValueLogStatusValue] = _valueLogStatus.get(valueLogStatusKey)

  private def deduplicateFileMetadataKey(fileMetadataKey: FileMetadataKey): FileMetadataKey = {
    if (!_fileMetadataKeyRepository.contains(fileMetadataKey)) {
      _fileMetadataKeyRepository += fileMetadataKey -> fileMetadataKey
    }
    _fileMetadataKeyRepository(fileMetadataKey)
  }

  def delete(key: Any): Unit = {
    ???
  }

  def exists(key: Any): Boolean = {
    ???
  }

  def write[K, V](key: K, value: V): Unit = {
    ???
  }

}

