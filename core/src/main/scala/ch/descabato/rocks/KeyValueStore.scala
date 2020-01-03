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
import org.rocksdb.ColumnFamilyDescriptor
import org.rocksdb.ColumnFamilyHandle
import org.rocksdb.ColumnFamilyOptions
import org.rocksdb.CompressionOptions
import org.rocksdb.CompressionType
import org.rocksdb.DBOptions
import org.rocksdb.OptimisticTransactionDB
import org.rocksdb.Options
import org.rocksdb.ReadOptions
import org.rocksdb.RocksDB
import org.rocksdb.Transaction
import org.rocksdb.WriteOptions
import scalapb.GeneratedMessage

import scala.collection.mutable
import scala.jdk.CollectionConverters._

object RocksDbKeyValueStore {
  def apply(rocksEnvInit: RocksEnvInit): RocksDbKeyValueStore = {
    val options = new Options()
    options.setCreateIfMissing(true)
    options.setCreateMissingColumnFamilies(true)
    options.setCompressionType(CompressionType.ZSTD_COMPRESSION)

    new RocksDbKeyValueStore(options, rocksEnvInit.rocksFolder, rocksEnvInit.readOnly)
  }
}

class RocksDbKeyValueStore(options: Options, path: File, readOnly: Boolean) extends LazyLogging {

  private var alreadyClosed: Boolean = false

  private val handles: util.List[ColumnFamilyHandle] = new util.ArrayList[ColumnFamilyHandle]()

  private val descriptors: util.List[ColumnFamilyDescriptor] = new util.ArrayList[ColumnFamilyDescriptor]()
  descriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY))

  private val revisionContentColumnFamily = new RevisionContentColumnFamily()

  private val chunkColumnFamily = new ChunkColumnFamily()
  private val fileMetadataColumnFamily = new FileMetadataColumnFamily()
  private val fileStatusColumnFamily = new FileStatusColumnFamily()
  private val revisionColumnFamily = new RevisionColumnFamily()

  private val columnFamilies = Seq(
    chunkColumnFamily,
    fileMetadataColumnFamily,
    fileStatusColumnFamily,
    revisionColumnFamily,
    revisionContentColumnFamily,
  )
  columnFamilies.foreach(x => descriptors.add(x.descriptor))

  private val db: RocksDB = {
    if (readOnly) {
      RocksDB.openReadOnly(new DBOptions(options), path.getAbsolutePath, descriptors, handles)
    } else {
      OptimisticTransactionDB.open(new DBOptions(options), path.getAbsolutePath, descriptors, handles)
    }
  }

  private val defaultWriteOptions = {
    new WriteOptions().setSync(true)
  }

  private val defaultReadOptions = new ReadOptions()

  handles.asScala.tail.zipWithIndex.foreach { case (handle, index) =>
    columnFamilies(index).handle = handle
  }

  private val existsTiming = new StopWatch("exists")
  private val writeTiming = new StopWatch("write")

  private var currentNumber: Long = {
    readAllUpdates().map(_._1.number + 1).maxOption.getOrElse(0L)
  }

  private var transaction: Transaction = null
  openTransaction()

  private def openTransaction(): Unit = {
    if (!readOnly) {
      transaction = db.asInstanceOf[OptimisticTransactionDB].beginTransaction(defaultWriteOptions)
    }
  }

  private def lookupColumnFamily[K <: Key, V](key: Key): ColumnFamily[K, V] = {
    columnFamilies.filter(_.canHandle(key)).head.asInstanceOf[ColumnFamily[K, V]]
  }

  /**
   * Warning: This will not read uncommitted data.
   *
   * @tparam K The key type
   * @tparam V the value type
   * @return An iterator over this
   */
  private def iterateKeysFrom[K <: Key, V](cf: ColumnFamily[K, V]): Seq[(K, V)] = {
    val iterator = db.newIterator(cf.handle)
    iterator.seekToFirst()
    val out = mutable.Buffer.empty[(K, V)]
    while (iterator.isValid) {
      out.append((cf.decodeKey(iterator.key()), cf.decodeValue(iterator.value())))
      iterator.next()
    }
    iterator.close()
    out.toSeq
  }

  def getAllChunks(): Seq[(ChunkKey, ValueLogIndex)] = {
    iterateKeysFrom[ChunkKey, ValueLogIndex](chunkColumnFamily)
  }

  def getAllRevisions(): Seq[(Revision, RevisionValue)] = {
    iterateKeysFrom[Revision, RevisionValue](revisionColumnFamily).sortBy(_._1.number)
  }

  def getAllValueLogStatusKeys(): Seq[(ValueLogStatusKey, ValueLogStatusValue)] = {
    iterateKeysFrom[ValueLogStatusKey, ValueLogStatusValue](fileStatusColumnFamily)
  }

  def readAllUpdates(): Seq[(RevisionContentKey, RevisionContentValue)] = {
    iterateKeysFrom[RevisionContentKey, RevisionContentValue](revisionContentColumnFamily)
  }

  private def writeRevisionValueContent(value: RevisionContentValue): Unit = {
    val keyEncoded = revisionContentColumnFamily.encodeKey(RevisionContentKey(currentNumber))
    val valueEncoded = revisionContentColumnFamily.encodeValue(value)
    logger.trace(s"Key for $currentNumber is ${Hex.toHexString(keyEncoded)}, value is ${decodeRevisionContentValue(value)}")
    transaction.put(revisionContentColumnFamily.handle, keyEncoded, valueEncoded.asArray())
    currentNumber += 1
  }

  def write[K <: Key, V](key: K, value: V, writeAsUpdate: Boolean = true): Unit = {
    def writeImpl(cf: ColumnFamily[K, V], key: K, value: V) = {
      val keyEncoded = cf.encodeKey(key)
      val valueEncoded = cf.encodeValue(value)
      transaction.put(cf.handle, keyEncoded, valueEncoded.asArray())
      (keyEncoded, valueEncoded)
    }

    writeTiming.measure {
      val cf = lookupColumnFamily[K, V](key)
      val (keyBytes, valueBytes) = writeImpl(cf, key, value)
      if (writeAsUpdate) {
        val revisionContentValue = RevisionContentValue.createUpdate(cf.ordinal, keyBytes, valueBytes)
        writeRevisionValueContent(revisionContentValue)
      }
    }
  }

  def exists[K <: Key](key: K): Boolean = {
    existsTiming.measure {
      val cf = lookupColumnFamily[K, Any](key)
      val existingValue = transaction.get(cf.handle, defaultReadOptions, cf.encodeKey(key))
      existingValue != null
    }
  }

  /**
   * Warning: these keys and values are not exported to the metadata
   *
   * @param key
   * @return
   */
  def readDefault(key: Array[Byte]): Array[Byte] = {
    if (readOnly) {
      db.get(key)
    } else {
      transaction.get(defaultReadOptions, key)
    }
  }

  /**
   * Warning: these keys and values are not exported to the metadata
   *
   * @param key
   * @return
   */
  def writeDefault(key: Array[Byte], value: Array[Byte]): Unit = {
    transaction.put(key, value)
  }

  def readRevision(revision: Revision): Option[RevisionValue] = {
    read(revision)
  }

  def readChunk(chunkKey: ChunkKey): Option[ValueLogIndex] = {
    read(chunkKey)
  }

  def readFileMetadata(fileMetadataKeyWrapper: FileMetadataKeyWrapper): Option[FileMetadataValue] = {
    read(fileMetadataKeyWrapper)
  }

  def readValueLogStatus(key: ValueLogStatusKey): Option[ValueLogStatusValue] = {
    read(key)
  }

  private def read[K <: Key, V](key: K): Option[V] = {
    val cf = lookupColumnFamily[K, V](key)
    val bytes: Array[Byte] = if (readOnly) {
      db.get(cf.handle, cf.encodeKey(key))
    } else {
      transaction.get(cf.handle, defaultReadOptions, cf.encodeKey(key))
    }
    Option(bytes)
      .map(cf.decodeValue)
  }

  def decodeRevisionContentValue(y: RevisionContentValue): (Boolean, Key, Option[Product]) = {
    val cf = columnFamilies.find(_.ordinal == y.ordinal).get
    val key = cf.decodeKey(y.key)
    if (y.deletion) {
      (true, key, None)
    } else {
      (false, key, Some(cf.decodeValue(y.value.asArray())))
    }
  }

  type Callback = () => Unit
  private var callbacksOnNextCommit: Seq[Callback] = Seq.empty

  def callbackOnNextCommit(callback: Callback): Unit = {
    callbacksOnNextCommit :+= callback
  }

  def commit(): Unit = {
    logger.info(s"Committing to rocksdb: ${transaction.getNumPuts} writes and ${transaction.getNumDeletes} deletions")
    transaction.commit()
    transaction.close()
    for (callback <- callbacksOnNextCommit) {
      callback.apply()
    }
    callbacksOnNextCommit = Seq.empty
    openTransaction()
  }

  def rollback(): Unit = {
    transaction.rollback()
    transaction.close()
    openTransaction()
  }

  def delete[K <: Key](key: K, writeAsUpdate: Boolean = true): Unit = {
    logger.trace(s"Deleting ${key} from rocksdb and write it as update? ${writeAsUpdate}")
    val cf = lookupColumnFamily[K, Any](key)
    val encodedKey = cf.encodeKey(key)
    transaction.delete(cf.handle, encodedKey)
    if (writeAsUpdate) {
      writeRevisionValueContent(RevisionContentValue.createDelete(cf.ordinal, encodedKey))
    }
  }

  def compact(): Unit = {
    db.compactRange()
  }

  def close(): Unit = {
    this.synchronized {
      if (alreadyClosed) {
        logger.error("Rocksdb was already closed")
      } else {
        if (transaction != null && transaction.getNumDeletes + transaction.getNumPuts > 0) {
          throw new IllegalStateException("Transaction must be rolled back or committed before closing db")
        }
        if (!readOnly) {
          db.syncWal()
          compact()
        }
        db.closeE()
        alreadyClosed = true
      }
    }
  }
}

abstract class ColumnFamily[K <: Key, V] {

  def name: String

  def encodeKey(key: K): Array[Byte]

  def decodeKey(encoded: Array[Byte]): K

  def encodeValue(value: V): BytesWrapper

  def decodeValue(encoded: Array[Byte]): V

  var handle: ColumnFamilyHandle = null

  def ordinal: Byte

  def isThisFamily(array: Array[Byte]): Boolean = array(0) == ordinal

  def canHandle(key: Key): Boolean

  def columnFamilyOptions: ColumnFamilyOptions = {
    val options = new CompressionOptions()
      .setMaxDictBytes(102400)
      .setZStdMaxTrainBytes(102400)
      .setEnabled(true)
    new ColumnFamilyOptions()
      .setCompressionType(CompressionType.ZSTD_COMPRESSION)
      .setCompressionOptions(options)
  }

  def descriptor: ColumnFamilyDescriptor = new ColumnFamilyDescriptor(name.getBytes(), columnFamilyOptions)
}

trait MessageValueColumnFamily[K <: Key, V <: GeneratedMessage] extends ColumnFamily[K, V] {
  override def encodeValue(value: V): BytesWrapper = {
    value.toByteArray.wrap()
  }
}

class RevisionColumnFamily extends ColumnFamily[Revision, RevisionValue] with MessageValueColumnFamily[Revision, RevisionValue] {
  override def name: String = "revision"

  override def canHandle(key: Key): Boolean = key match {
    case Revision(_) => true
    case _ => false
  }

  override def encodeKey(key: Revision): Array[Byte] = {
    Integer.toString(key.number).getBytes()
  }

  override def decodeKey(encoded: Array[Byte]): Revision = {
    Revision(new String(encoded).toInt)
  }

  override def decodeValue(encoded: Array[Byte]): RevisionValue = {
    RevisionValue.parseFrom(CompressedStream.decompressToBytes(encoded.wrap()).asArray())
  }

  override def encodeValue(value: RevisionValue): BytesWrapper = {
    CompressedStream.compress(super.encodeValue(value), CompressionMode.gzip)
  }

  override def ordinal: Byte = 0
}

class FileMetadataColumnFamily extends ColumnFamily[FileMetadataKeyWrapper, FileMetadataValue] with MessageValueColumnFamily[FileMetadataKeyWrapper, FileMetadataValue] {
  override def name: String = "filemetadata"

  override def canHandle(key: Key): Boolean = key match {
    case FileMetadataKeyWrapper(_) => true
    case _ => false
  }

  override def encodeKey(key: FileMetadataKeyWrapper): Array[Byte] = {
    key.fileMetadataKey.toByteArray
  }

  override def decodeKey(encoded: Array[Byte]): FileMetadataKeyWrapper = {
    FileMetadataKeyWrapper(FileMetadataKey.parseFrom(encoded))
  }

  override def decodeValue(encoded: Array[Byte]): FileMetadataValue = {
    FileMetadataValue.parseFrom(encoded)
  }

  override def ordinal: Byte = 1
}

class ChunkColumnFamily extends ColumnFamily[ChunkKey, ValueLogIndex] with MessageValueColumnFamily[ChunkKey, ValueLogIndex] {
  override def name: String = "chunks"

  override def canHandle(key: Key): Boolean = key match {
    case ChunkKey(_) => true
    case _ => false
  }

  override def encodeKey(key: ChunkKey): Array[Byte] = {
    key.hash.bytes
  }

  override def decodeKey(encoded: Array[Byte]): ChunkKey = {
    ChunkKey(Hash(encoded))
  }

  override def decodeValue(encoded: Array[Byte]): ValueLogIndex = {
    ValueLogIndex.parseFrom(encoded)
  }

  override def ordinal: Byte = 3
}

class FileStatusColumnFamily extends ColumnFamily[ValueLogStatusKey, ValueLogStatusValue] with MessageValueColumnFamily[ValueLogStatusKey, ValueLogStatusValue] {
  override def name: String = "filestatus"

  override def canHandle(key: Key): Boolean = key match {
    case ValueLogStatusKey(_) => true
    case _ => false
  }

  override def encodeKey(key: ValueLogStatusKey): Array[Byte] = {
    key.name.getBytes(StandardCharsets.UTF_8)
  }

  override def decodeKey(encoded: Array[Byte]): ValueLogStatusKey = {
    ValueLogStatusKey(new String(encoded, StandardCharsets.UTF_8))
  }

  override def decodeValue(encoded: Array[Byte]): ValueLogStatusValue = {
    ValueLogStatusValue.parseFrom(encoded)
  }

  override def ordinal: Byte = 16
}

class RevisionContentColumnFamily extends ColumnFamily[RevisionContentKey, RevisionContentValue] {
  override def name: String = "revisionContent"

  override def canHandle(key: Key): Boolean = key match {
    case RevisionContentKey(_) => true
    case _ => false
  }

  override def encodeKey(key: RevisionContentKey): Array[Byte] = {
    val buffer = ByteBuffer.allocate(java.lang.Long.BYTES)
    buffer.putLong(key.number)
    buffer.array()
  }

  override def decodeKey(encoded: Array[Byte]): RevisionContentKey = {
    val buffer = ByteBuffer.allocate(java.lang.Long.BYTES)
    buffer.put(encoded)
    buffer.flip()
    val key = buffer.getLong
    RevisionContentKey(key)
  }

  override def decodeValue(encoded: Array[Byte]): RevisionContentValue = {
    RevisionContentValue.decode(encoded)
  }

  override def encodeValue(value: RevisionContentValue): BytesWrapper = {
    value.asArray()
  }

  override def ordinal: Byte = Byte.MaxValue
}
