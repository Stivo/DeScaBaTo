package ch.descabato.core.model

import ch.descabato.core.model
import ch.descabato.core.util.InMemoryDbOld
import ch.descabato.protobuf.keys.FileMetadataKey
import ch.descabato.protobuf.keys.FileMetadataValue
import ch.descabato.protobuf.keys.RevisionValue
import ch.descabato.protobuf.keys.ValueLogIndex
import ch.descabato.protobuf.keys.ValueLogStatusValue
import ch.descabato.utils.Hash
import com.typesafe.scalalogging.LazyLogging
import scalapb.GeneratedMessage

object KeyValueStore {
  def apply(backupEnvInit: BackupEnvInit): KeyValueStore = {
    new KeyValueStore(backupEnvInit.readOnly)
  }
}

class KeyValueStore(readOnly: Boolean, private val inMemoryDb: InMemoryDbOld = new InMemoryDbOld()) extends LazyLogging {

  def getAllChunks(): Map[ChunkKey, ValueLogIndex] = {
    inMemoryDb.chunks
  }

  def getAllRevisions(): Map[RevisionKey, RevisionValue] = {
    inMemoryDb.revision
  }

  def getAllValueLogStatusKeys(): Map[ValueLogStatusKey, ValueLogStatusValue] = {
    inMemoryDb.valueLogStatus
  }

  def getIndexes(fileMetadataValue: FileMetadataValue): Iterator[ValueLogIndex] = {
    for {
      hash <- getHashes(fileMetadataValue)
    } yield readChunk(hash).get
  }

  def getHashes(fileMetadataValue: FileMetadataValue): Iterator[ChunkKey] = {
    for {
      hashBytes <- fileMetadataValue.hashes.asArray().grouped(32)
    } yield ChunkKey(Hash(hashBytes))
  }

  def getAllEntriesAsUpdates(): Unit = {
    ???
  }

  def writeRevision(key: RevisionKey, value: RevisionValue): Unit = {
    ensureOpenForWriting()
    inMemoryDb.write(key, value)
    // TODO rewrite track updates
  }

  def writeFileMetadata(key: FileMetadataKey, value: FileMetadataValue): Unit = {
    ensureOpenForWriting()
    inMemoryDb.write(key, value)
    // TODO rewrite track updates
  }

  def writeChunk(key: ChunkKey, value: ValueLogIndex): Unit = {
    ensureOpenForWriting()
    inMemoryDb.write(key, value)
    // TODO rewrite track updates
  }

  def writeStatus(key: ValueLogStatusKey, value: ValueLogStatusValue): Unit = {
    ensureOpenForWriting()
    inMemoryDb.write(key, value)
    // TODO rewrite track updates
  }

  def existsChunk(key: ChunkKey): Boolean = {
    inMemoryDb.exists(key)
  }

  def existsFileMetadata(key: FileMetadataKey): Boolean = {
    inMemoryDb.exists(key)
  }

  def readRevision(revision: RevisionKey): Option[RevisionValue] = {
    inMemoryDb.readRevision(revision)
  }

  def readChunk(chunkKey: ChunkKey): Option[ValueLogIndex] = {
    inMemoryDb.readChunk(chunkKey)
  }

  def readFileMetadata(fileMetadataKey: FileMetadataKey): Option[FileMetadataValue] = {
    inMemoryDb.readFileMetadata(fileMetadataKey)
  }

  def readValueLogStatus(key: ValueLogStatusKey): Option[ValueLogStatusValue] = {
    inMemoryDb.readValueLogStatus(key)
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
