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

class KvStoreStorageMechanismWriter(val file: File, val passphrase: Option[String] = None) extends StorageMechanismWriter[KvStoreLocation, Array[Byte], Array[Byte]] {
  lazy val kvstoreWriter = KvStore.makeWriterType2(file, if (passphrase.isDefined) passphrase.get else null)

  def add(k: Array[Byte], v: Array[Byte]) {
    kvstoreWriter.writeKeyValue(k, v)
  }

  def checkpoint(): Unit = {
    kvstoreWriter.checkpoint()
    // TODO kvstoreWriter.checkpoint()
  }

  def close(): Unit = {
    kvstoreWriter.close()
  }

  def length() = {
    kvstoreWriter.length()
  }
}

case class KvStoreLocation(val file: File, val pos: Long) extends Location

class KvStoreStorageMechanismReader(val file: File, val passphrase: Option[String] = None) extends StorageMechanismReader[KvStoreLocation, Array[Byte], Array[Byte]] {
  lazy val kvstoreReader = KvStore.makeReaderWithPassphrase(file, if (passphrase.isDefined) passphrase.get else null)
  
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
