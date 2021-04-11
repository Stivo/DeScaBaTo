package ch.descabato.rocks

import java.io.File
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util

import ch.descabato.CompressionMode
import ch.descabato.rocks.protobuf.keys.FileMetadataKey
import ch.descabato.rocks.protobuf.keys.FileMetadataValue
import ch.descabato.rocks.protobuf.keys.RevisionValue
import ch.descabato.rocks.protobuf.keys.ValueLogIndex
import ch.descabato.rocks.protobuf.keys.ValueLogStatusValue
import ch.descabato.utils.BytesWrapper
import ch.descabato.utils.CompressedStream
import ch.descabato.utils.Hash
import ch.descabato.utils.Implicits._
import com.typesafe.scalalogging.LazyLogging
import org.bouncycastle.util.encoders.Hex
import scalapb.GeneratedMessage

import scala.collection.mutable
import scala.jdk.CollectionConverters._

object KeyValueStore {
  def apply(rocksEnvInit: RocksEnvInit): KeyValueStore = {
    new KeyValueStore(rocksEnvInit.readOnly)
  }
}

class KeyValueStore(readOnly: Boolean) extends LazyLogging {

  def getAllFileMetadatas(): Seq[(FileMetadataKeyWrapper, FileMetadataValue)] = {
    ???
  }

  def getAllChunks(): Seq[(ChunkKey, ValueLogIndex)] = {
    ???
  }

  def getAllRevisions(): Seq[(Revision, RevisionValue)] = {
    ???
  }

  def getAllValueLogStatusKeys(): Seq[(ValueLogStatusKey, ValueLogStatusValue)] = {
    ???
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

  def readAllUpdates(): Seq[(RevisionContentKey, RevisionContentValue)] = {
    ???
  }

  def write[K <: Key, V](key: K, value: V, writeAsUpdate: Boolean = true): Unit = {
    ensureOpenForWriting()

    ???
  }

  def exists[K <: Key](key: K): Boolean = {
    ???
  }

  /**
   * Warning: these keys and values are not exported to the metadata
   *
   * @param key
   * @return
   */
  def readDefault(key: Array[Byte]): Array[Byte] = {
    if (readOnly) {
      ???
    } else {
      ???
    }
  }

  /**
   * Warning: these keys and values are not exported to the metadata
   *
   * @param key
   * @return
   */
  def writeDefault(key: Array[Byte], value: Array[Byte]): Unit = {
    ???
  }

  def readRevision(revision: Revision): Option[RevisionValue] = {
    ???
  }

  def readChunk(chunkKey: ChunkKey): Option[ValueLogIndex] = {
    ???
  }

  def readFileMetadata(fileMetadataKeyWrapper: FileMetadataKeyWrapper): Option[FileMetadataValue] = {
    ???
  }

  def readValueLogStatus(key: ValueLogStatusKey): Option[ValueLogStatusValue] = {
    ???
  }

  type Callback = () => Unit
  private var callbacksOnNextCommit: Seq[Callback] = Seq.empty

  def callbackOnNextCommit(callback: Callback): Unit = {
    callbacksOnNextCommit :+= callback
  }

  def commit(): Unit = {
    ensureOpenForWriting()
    //    logger.info(s"Committing to rocksdb: ${transaction.getNumPuts} writes and ${transaction.getNumDeletes} deletions")
    //    transaction.commit()
    //    transaction.close()
    //    for (callback <- callbacksOnNextCommit) {
    //      callback.apply()
    //    }
    //    callbacksOnNextCommit = Seq.empty
    //    openTransaction()
  }

  private def ensureOpenForWriting() = {
    if (readOnly) {
      throw new IllegalArgumentException("Rocks is opened in readonly mode")
    }
  }

  def delete[K <: Key](key: K): Unit = {
    ensureOpenForWriting()
    ???
  }

  def close(): Unit = {
    ???
    //    this.synchronized {
    //      if (alreadyClosed) {
    //        logger.error("Rocksdb was already closed")
    //      } else {
    //        if (transaction != null && transaction.getNumDeletes + transaction.getNumPuts > 0) {
    //          throw new IllegalStateException("Transaction must be rolled back or committed before closing db")
    //        }
    //        if (!readOnly) {
    //          db.syncWal()
    //          compact()
    //        }
    //        db.closeE()
    //        alreadyClosed = true
    //      }
    //    }
  }
}
