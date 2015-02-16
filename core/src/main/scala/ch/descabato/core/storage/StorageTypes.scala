package ch.descabato.core.storage

import java.io.File
import java.util

import ch.descabato.core.{MetaInfo, UniversePart}
import ch.descabato.core.kvstore.{Entry, KvStoreReaderImpl, KvStoreWriterImpl}
import ch.descabato.utils.JsonSerialization

object StorageMechanismConstants {
  val manifestName = "manifest.txt".getBytes
}

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

class KvStoreStorageMechanismWriter(val file: File, val passphrase: Option[String] = None)
    extends StorageMechanismWriter[KvStoreLocation, Array[Byte], Array[Byte]] with UniversePart {

  lazy val kvstoreWriter = new KvStoreWriterImpl(file, if (passphrase.isDefined) passphrase.get else null)

  def add(k: Array[Byte], v: Array[Byte]) {
    kvstoreWriter.writeKeyValue(k, v)
  }

  def checkpoint(): Unit = {
    kvstoreWriter.checkpoint()
  }

  def close(): Unit = {
    kvstoreWriter.close()
  }

  def length() = {
    kvstoreWriter.length()
  }

  def writeManifest() {
    val versionNumber: String = ch.descabato.version.BuildInfo.version
    val m = new MetaInfo(universe.fileManager().getDateFormatted, versionNumber)
    val json = new JsonSerialization()
    val value = json.write(m)
    kvstoreWriter.writeKeyValue(StorageMechanismConstants.manifestName, value)
  }

}

case class KvStoreLocation(val file: File, val pos: Long) extends Location

class KvStoreStorageMechanismReader(val file: File, val passphrase: Option[String] = None) extends StorageMechanismReader[KvStoreLocation, Array[Byte], Array[Byte]] {
  lazy val kvstoreReader = new KvStoreReaderImpl(file, if (passphrase.isDefined) passphrase.get else null)

  private var _manifest: MetaInfo = null
  private var _manifestReadFailed = false

  def iterator = {
    kvstoreReader.iterator.collect {
      case Entry(_, k::v::Nil) if !util.Arrays.equals(k.array, StorageMechanismConstants.manifestName) => (k.array, new KvStoreLocation(file, v.startPos))
    }
  }

  def manifest() = {
    if (_manifest == null && !_manifestReadFailed) {
      kvstoreReader.iterator.find {
        case Entry(_, k :: v :: Nil) if util.Arrays.equals(k.array, StorageMechanismConstants.manifestName) =>
          true
      }.foreach { e =>
        val json = new JsonSerialization()
        json.read[MetaInfo](e.parts.last.array) match {
          case Left(m) => _manifest = m
          case _ => _manifestReadFailed = true
        }
      }
    }
    _manifest
  }
  
  def get(loc: KvStoreLocation) = {
    kvstoreReader.readEntryPartAt(loc.pos).array
  } 

  def close(): Unit = {
    kvstoreReader.close()
  }
}
