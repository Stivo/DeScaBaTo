package ch.descabato.core.storage

import java.io.File
import java.util

import ch.descabato.core.{MetaInfo, UniversePart}
import ch.descabato.core.kvstore.{Entry, KvStoreReaderImpl, KvStoreWriterImpl}
import ch.descabato.utils.{BytesWrapper, JsonSerialization}
import ch.descabato.utils.Implicits._

object StorageMechanismConstants {
  val manifestName: Array[Byte] = "manifest.txt".getBytes
  val manifestNameWrapped: BytesWrapper = manifestName.wrap()
}

trait IndexMechanism[K, L <: Location] {
  def getLocationForEntry(k: K): L
  def addToIndex(k: K, l: L)
  def containsEntry(k: K): Boolean
}

trait Location

trait StorageMechanismWriter[L <: Location, K, V] extends AutoCloseable {
  def add(k: K, v: V): L
  def checkpoint()
}

trait StorageMechanismReader[L <: Location, K, V] extends AutoCloseable with Iterable[(K, L)] {
  def get(l: L): V
}

class KvStoreStorageMechanismWriter(val file: File, val passphrase: Option[String] = None)
    extends StorageMechanismWriter[KvStoreLocation, Array[Byte], BytesWrapper] with UniversePart {

  lazy val kvstoreWriter = new KvStoreWriterImpl(file, if (passphrase.isDefined) passphrase.get else null)

  def add(k: Array[Byte], v: BytesWrapper): KvStoreLocation = {
    kvstoreWriter.writeKeyValue(k, v)
  }

  def checkpoint(): Unit = {
    kvstoreWriter.checkpoint()
  }

  def close(): Unit = {
    kvstoreWriter.close()
  }

  def length(): Long = {
    kvstoreWriter.length()
  }

  def writeManifest() {
    val versionNumber: String = ch.descabato.version.BuildInfo.version
    val m = MetaInfo(universe.fileManager().getDateFormatted, versionNumber)
    val json = new JsonSerialization()
    val value = json.write(m)
    kvstoreWriter.writeKeyValue(StorageMechanismConstants.manifestName, value.wrap())
  }

}

case class KvStoreLocation(file: File, pos: Long) extends Location

class KvStoreStorageMechanismReader(val file: File, val passphrase: Option[String] = None) extends StorageMechanismReader[KvStoreLocation, Array[Byte], BytesWrapper] {
  lazy val kvstoreReader = new KvStoreReaderImpl(file, if (passphrase.isDefined) passphrase.get else null)

  private var _manifest: MetaInfo = _
  private var _manifestReadFailed = false

  def iterator: Iterator[(Array[Byte], KvStoreLocation)] = {
    kvstoreReader.iterator.collect {
      case Entry(_, k::v::Nil)
        if ! (StorageMechanismConstants.manifestNameWrapped == k.bytes)
        => (k.bytes.asArray(), KvStoreLocation(file, v.startPos))
    }
  }

  def manifest(): MetaInfo = {
    if (_manifest == null && !_manifestReadFailed) {
      kvstoreReader.iterator.find {
        case Entry(_, k :: v :: Nil)
          if StorageMechanismConstants.manifestNameWrapped == k.bytes =>
            true
      }.foreach { e =>
        val json = new JsonSerialization()
        json.read[MetaInfo](e.parts.last.bytes) match {
          case Left(m) => _manifest = m
          case _ => _manifestReadFailed = true
        }
      }
    }
    _manifest
  }
  
  def get(loc: KvStoreLocation): BytesWrapper = {
    kvstoreReader.readEntryPartAt(loc.pos).bytes
  } 

  def close(): Unit = {
    kvstoreReader.close()
  }
}
