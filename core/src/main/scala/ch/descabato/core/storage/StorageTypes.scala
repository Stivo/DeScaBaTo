package ch.descabato.core.storage

import java.io.File
import ch.descabato.core.kvstore.KvStore
import ch.descabato.core.kvstore.Entry

trait IndexMechanism[K, L <: Location] {
  def getLocationForEntry(k: K): L
  def addToIndex(k: K, l: L)
  def containsEntry(k: K): Boolean
}

trait Location

trait StorageMechanismWriter[L <: Location, K, V] extends AutoCloseable {
  def add(k: K, v: V)
  def checkpoint()
}

trait StorageMechanismReader[L <: Location, K, V] extends AutoCloseable with Iterable[(K, L)] {
  def get(l: L): V
}

class KvStoreStorageMechanismWriter(val file: File) extends StorageMechanismWriter[KvStoreLocation, Array[Byte], Array[Byte]] {
  lazy val kvstoreWriter = KvStore.makeWriterType0(file)

  def add(k: Array[Byte], v: Array[Byte]) {
    kvstoreWriter.writeKeyValue(k, v)
  }

  def checkpoint(): Unit = {
    // TODO kvstoreWriter.checkpoint()
  }

  def close(): Unit = {
    kvstoreWriter.close()
  }
}

class KvStoreLocation(val file: File, val pos: Long) extends Location

class KvStoreStorageMechanismReader(val file: File) extends StorageMechanismReader[KvStoreLocation, Array[Byte], Array[Byte]] {
  lazy val kvstoreReader = KvStore.makeReader(file)
  
  def iterator = {
    kvstoreReader.iterator().collect{case Entry(_, k::v::Nil) => (k.array, new KvStoreLocation(file, v.startPos))}
  }
  
  def get(loc: KvStoreLocation) = {
    kvstoreReader.readEntryPartAt(loc.pos).array
  } 

  def close(): Unit = {
    kvstoreReader.close()
  }
}
