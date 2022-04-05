package ch.descabato.core.model

import ch.descabato.core.util.InMemoryDb
import ch.descabato.protobuf.keys.FileMetadataKey
import ch.descabato.protobuf.keys.FileMetadataValue
import ch.descabato.protobuf.keys.ProtoDb
import ch.descabato.protobuf.keys.RevisionValue
import ch.descabato.protobuf.keys.ValueLogIndex
import ch.descabato.protobuf.keys.ValueLogStatusValue
import com.typesafe.scalalogging.LazyLogging

object KeyValueStore {
  def apply(backupEnvInit: BackupEnvInit): KeyValueStore = {
    new KeyValueStore(backupEnvInit.readOnly)
  }
}

class KeyValueStore(readOnly: Boolean, private var inMemoryDb: InMemoryDb = InMemoryDb.empty) extends LazyLogging {

  private var updates = new ProtoDb()

  def getUpdates(): ProtoDb = updates

  def getAllChunks(): Iterator[(ChunkKey, ValueLogIndex)] = {
    inMemoryDb.chunkMap.iterator()
  }

  def getAllRevisions(): Map[RevisionKey, RevisionValue] = {
    inMemoryDb.revisions
  }

  def getAllValueLogStatusKeys(): Map[ValueLogStatusKey, ValueLogStatusValue] = {
    inMemoryDb.valueLogStatus
  }

  def getAllFileMetadata(): Iterator[(FileMetadataKey, FileMetadataValue)] = {
    inMemoryDb.fileMetadataMap.iterator()
  }

  def getIndexes(fileMetadataValue: FileMetadataValue): Iterator[ValueLogIndex] = {
    fileMetadataValue.hashIds.iterator.flatMap { x =>
      inMemoryDb.chunkMap.getByKeyId(x)
    }.map(_._2)
  }

  def writeRevision(key: RevisionKey, value: RevisionValue): Unit = {
    ensureOpenForWriting()
    inMemoryDb.addRevision(key, value)
    updates = updates.addRevisions((key, value))
  }

  def writeFileMetadata(key: FileMetadataKey, value: FileMetadataValue): FileMetadataId = {
    ensureOpenForWriting()
    val keyId = inMemoryDb.fileMetadataMap.add(key, value)
    updates = updates.copy(fileMetadataMap = updates.fileMetadataMap.addFileMetadataKeys((keyId, key)))
    updates = updates.copy(fileMetadataMap = updates.fileMetadataMap.addFileMetadataValues((keyId, value)))
    keyId
  }

  def writeChunk(key: ChunkKey, value: ValueLogIndex): ChunkId = {
    ensureOpenForWriting()
    val keyId = inMemoryDb.chunkMap.add(key, value)
    updates = updates.copy(chunkMap = updates.chunkMap.addChunkKeys((keyId, key)))
    updates = updates.copy(chunkMap = updates.chunkMap.addChunkValues((keyId, value)))
    keyId
  }

  def writeStatus(key: ValueLogStatusKey, value: ValueLogStatusValue): Unit = {
    ensureOpenForWriting()
    inMemoryDb.addValueLogStatus(key, value)
    updates = updates.addStatus((key, value))
  }

  def existsFileMetadata(key: FileMetadataKey): Boolean = {
    getFileMetadataByKey(key).isDefined
  }

  def getFileMetadataByKey(key: FileMetadataKey): Option[(FileMetadataId, FileMetadataValue)] = {
    inMemoryDb.fileMetadataMap.getByKey(key)
  }

  def getFileMetadataByKeyId(keyId: FileMetadataId): Option[(FileMetadataKey, FileMetadataValue)] = {
    inMemoryDb.fileMetadataMap.getByKeyId(keyId)
  }

  def readRevision(revision: RevisionKey): Option[RevisionValue] = {
    inMemoryDb.getRevision(revision)
  }

  def getChunk(chunkKey: ChunkKey): Option[(ChunkId, ValueLogIndex)] = inMemoryDb.chunkMap.getByKey(chunkKey)

  def getChunkById(chunkKey: ChunkId): Option[(ChunkKey, ValueLogIndex)] = inMemoryDb.chunkMap.getByKeyId(chunkKey)

  def readValueLogStatus(key: ValueLogStatusKey): Option[ValueLogStatusValue] = {
    inMemoryDb.getValueLogStatus(key)
  }

  // TODO review: this does nothing right now. Might have to rethink all the parts that use this
  def commit(): Unit = {
    ensureOpenForWriting()
  }

  private def ensureOpenForWriting(): Unit = {
    if (readOnly) {
      throw new IllegalArgumentException("Rocks is opened in readonly mode")
    }
  }

}
