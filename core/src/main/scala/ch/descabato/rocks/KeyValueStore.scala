package ch.descabato.rocks

import ch.descabato.rocks.protobuf.keys.FileMetadataKey
import ch.descabato.rocks.protobuf.keys.FileMetadataValue
import ch.descabato.rocks.protobuf.keys.RevisionValue
import ch.descabato.rocks.protobuf.keys.ValueLogIndex
import ch.descabato.rocks.protobuf.keys.ValueLogStatusValue
import ch.descabato.utils.Hash
import com.typesafe.scalalogging.LazyLogging
import scalapb.GeneratedMessage

object KeyValueStore {
  def apply(rocksEnvInit: RocksEnvInit): KeyValueStore = {
    new KeyValueStore(rocksEnvInit.readOnly)
  }
}

class KeyValueStore(readOnly: Boolean, private val inMemoryDb: InMemoryDb = new InMemoryDb()) extends LazyLogging {

  private var updates: Seq[ExportedEntry[_, _]] = Seq.empty

  def getAllFileMetadatas(): Seq[(FileMetadataKey, FileMetadataValue)] = {
    inMemoryDb.fileMetadata.toSeq
  }

  def getAllChunks(): Seq[(ChunkKey, ValueLogIndex)] = {
    inMemoryDb.chunks.toSeq
  }

  def getAllRevisions(): Seq[(Revision, RevisionValue)] = {
    inMemoryDb.revision.toSeq
  }

  def getAllValueLogStatusKeys(): Seq[(ValueLogStatusKey, ValueLogStatusValue)] = {
    inMemoryDb.valueLogStatus.toSeq
  }

  def getIndexes(fileMetadataValue: FileMetadataValue): Iterator[ValueLogIndex] = {
    for {
      hash <- getHashes(fileMetadataValue)
    } yield readChunk(hash).get
  }

  def getHashes(fileMetadataValue: FileMetadataValue): Iterator[ChunkKey] = {
    for {
      hashBytes <- fileMetadataValue.hashes.toByteArray.grouped(32)
    } yield ChunkKey(Hash(hashBytes))
  }

  def readAllUpdates(): Seq[ExportedEntry[_, _]] = {
    updates
  }

  def getAllEntriesAsUpdates(): Seq[ExportedEntry[_, _]] = {
    var out = Seq.empty[ExportedEntry[_, _]]
    inMemoryDb.valueLogStatus.foreach { case (k, v) =>
      out :+= ExportedEntry(k, v, false)
    }
    out ++= inMemoryDb.chunks.map { case (k, v) =>
      ExportedEntry(k, v, false)
    }
    inMemoryDb.revision.foreach { case (k, v) =>
      out :+= ExportedEntry(k, v, false)
    }
    out ++= inMemoryDb.fileMetadata.map { case (k, v) =>
      ExportedEntry(FileMetadataKeyWrapper(k), v, false)
    }
    out
  }

  def write[K <: Key, V <: GeneratedMessage](key: K, value: V): Unit = {
    ensureOpenForWriting()
    inMemoryDb.write(key, value)
    updates :+= ExportedEntry(key, value, false)
  }

  def exists[K <: Key](key: K): Boolean = {
    inMemoryDb.exists(key)
  }

  def readRevision(revision: Revision): Option[RevisionValue] = {
    inMemoryDb.readRevision(revision)
  }

  def readChunk(chunkKey: ChunkKey): Option[ValueLogIndex] = {
    inMemoryDb.readChunk(chunkKey)
  }

  def readFileMetadata(fileMetadataKeyWrapper: FileMetadataKeyWrapper): Option[FileMetadataValue] = {
    inMemoryDb.readFileMetadata(fileMetadataKeyWrapper)
  }

  def readValueLogStatus(key: ValueLogStatusKey): Option[ValueLogStatusValue] = {
    inMemoryDb.readValueLogStatus(key)
  }

  def commit(): Unit = {
    ensureOpenForWriting()
  }

  private def ensureOpenForWriting(): Unit = {
    if (readOnly) {
      throw new IllegalArgumentException("Rocks is opened in readonly mode")
    }
  }

  def delete[K <: Key](key: K): Unit = {
    ensureOpenForWriting()
    updates :+= ExportedEntry(key, null, true)
  }

}
