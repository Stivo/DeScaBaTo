package ch.descabato.core.model

import ch.descabato.core.util.InMemoryDb
import ch.descabato.protobuf.keys.FileMetadataKey
import ch.descabato.protobuf.keys.FileMetadataValue
import ch.descabato.protobuf.keys.ProtoDb
import ch.descabato.protobuf.keys.RevisionValue
import ch.descabato.protobuf.keys.ValueLogIndex
import ch.descabato.protobuf.keys.ValueLogStatusValue
import ch.descabato.utils.Hash
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
    inMemoryDb.chunkIterator
  }

  def getAllRevisions(): Map[RevisionKey, RevisionValue] = {
    inMemoryDb.revisions
  }

  def getAllValueLogStatusKeys(): Map[ValueLogStatusKey, ValueLogStatusValue] = {
    inMemoryDb.valueLogStatus
  }

  def getIndexes(fileMetadataValue: FileMetadataValue): Iterator[ValueLogIndex] = {
    fileMetadataValue.hashIds.iterator.map { x =>
      inMemoryDb.getChunkById(x).get
    }
  }

  def writeRevision(key: RevisionKey, value: RevisionValue): Unit = {
    ensureOpenForWriting()
    inMemoryDb.addRevision(key, value)
    updates = updates.addRevisions((key, value))
  }

  def writeFileMetadata(key: FileMetadataKey, value: FileMetadataValue): FileMetadataKeyId = {
    ensureOpenForWriting()
    val keyId = inMemoryDb.addFileMetadata(key, value)
    updates = updates.copy(fileMetadataMap = updates.fileMetadataMap.addFileMetadataKeys((keyId, key)))
    updates = updates.copy(fileMetadataMap = updates.fileMetadataMap.addFileMetadataValues((keyId, value)))
    keyId
  }

  def writeChunk(key: ChunkKey, value: ValueLogIndex): ChunkMapKeyId = {
    ensureOpenForWriting()
    val keyId = inMemoryDb.addChunk(key, value)
    updates = updates.copy(chunkMap = updates.chunkMap.addChunkKeys((keyId, key)))
    updates = updates.copy(chunkMap = updates.chunkMap.addChunkValues((keyId, value)))
    keyId
  }

  def writeStatus(key: ValueLogStatusKey, value: ValueLogStatusValue): Unit = {
    ensureOpenForWriting()
    inMemoryDb.addValueLogStatus(key, value)
    updates = updates.addStatus((key, value))
  }

  def existsChunk(key: ChunkKey): Boolean = {
    inMemoryDb.existsChunk(key)
  }

  def existsFileMetadata(key: FileMetadataKey): Boolean = {
    inMemoryDb.existsFileMetadata(key)
  }

  def readRevision(revision: RevisionKey): Option[RevisionValue] = {
    inMemoryDb.getRevision(revision)
  }

  def getChunkKeyId(chunkKey: ChunkKey): Option[ChunkMapKeyId] = inMemoryDb.getChunkKeyId(chunkKey)

  def readChunk(chunkKey: ChunkKey): Option[ValueLogIndex] = {
    inMemoryDb.getChunk(chunkKey)
  }

  def readFileMetadata(fileMetadataKey: FileMetadataKey): Option[FileMetadataValue] = {
    inMemoryDb.getFileMetadata(fileMetadataKey)
  }

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
