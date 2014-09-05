package ch.descabato.core.kvstore

import java.io.File
import ch.descabato.version.BuildInfo

import scala.collection.immutable.HashMap
import java.io.ByteArrayOutputStream
import java.io.DataOutputStream
import java.util.Arrays
import javax.crypto.Mac
import ch.descabato.core.{UniversePart, BaWrapper}
import ch.descabato.utils.{JsonSerialization, MetaInfo, Utils}

case class EntryType(val markerByte: Byte, val parts: Int)

class EncryptionInfo (
  // algorithm 0 is for AES/CTR/NoPadding
  val algorithm: Byte = 0,
  // algorithm 0 is for HmacSHA256
  val macAlgorithm: Byte = 0,
  val ivLength: Byte = 16,
  val iv: Array[Byte] = CryptoUtils newStrongRandomByteArray(16)
)

class KeyDerivationInfo (
  // algorithm 0 is for scrypt with 4 16 2 keyLength
  val algorithm: Byte = 0,
  val iterationsPower: Byte = 16,
  val memoryFactor: Byte = 8,
  val keyLength: Byte = 16,
  val saltLength: Byte = 20,
  val salt: Array[Byte] = CryptoUtils newStrongRandomByteArray(20)
)

object EntryTypes {
  val simple = new EntryType(0, 1)
  val keyValue = new EntryType(1, 2)
  val endOfFile = new EntryType(255.toByte, 0)
  def getTypeForByte(byte: Byte) = {
    byte match {
      case 0 => simple
      case 1 => keyValue
      case -1 => endOfFile
      case e => throw new IllegalArgumentException("Not a valid value for entrytype")
    }
  }

}

trait EntryPart {
  def array: Array[Byte]
  def hasCrc: Boolean
  /**
   * points to the marker byte is for this entrypart
   */
  def startPos: Long
  /**
   * points to the end of this entrypart
   * at endPos + 1 the next entry begins
   */
  def endPos: Long
}

class EntryPartW(val array: Array[Byte], val hasCrc: Boolean = true) extends EntryPart {
  var startPos = 0L
  def endPos = startPos + array.length
}

case class Entry(typ: EntryType, parts: Iterable[EntryPart]) {
  def this(typ: EntryType, parts: Iterable[EntryPart], startPosIn: Long) {
    this(typ, parts)
    startPos = startPosIn
  }
  def getEndOfEntryPos(): Long = {
    parts match {
      case Nil => startPos + 1
      case list => list.last.endPos
    }
  }
  var startPos: Long = -2
}

trait KvStoreWriter extends AutoCloseable {
  def writeEntry(entry: Entry)
  def writeKeyValue(key: Array[Byte], value: Array[Byte]) = {
    writeEntry(new Entry(EntryTypes.keyValue, newEntryPart(key, false) :: newEntryPart(value, false) :: Nil))
  }
  def newEntryPart(array: Array[Byte], hasCrc: Boolean = false) = {
    new EntryPartW(array, hasCrc)
  }
}

trait KvStoreReader extends AutoCloseable with Iterable[Entry] {
  def getPosOfFirstEntry(): Long
  def readEntryPartAt(pos: Long): EntryPart
  def readEntryAt(pos: Long): Option[Entry]
}

trait IndexedKvStoreReader extends KvStoreReader {
  lazy val index =  iterator.flatMap {
      case Entry(e, key::value::Nil) => Some((new BaWrapper(key.array), value.startPos))
      case _ => None
    }.toMap
    
  def readValueForKey(key: Array[Byte]) = {
    val pos = index(new BaWrapper(key))
    readEntryPartAt(pos).array
  }
}

class KvStoreWriterImpl(val file: File, val passphrase: String = null, val key: Array[Byte] = null, val fsyncThreshold: Long = 100*1024*1024) extends KvStoreWriter {
  lazy val encryptionInfo = new EncryptionInfo()
  lazy val keyDerivationInfo = new KeyDerivationInfo()
	lazy val writer = {
    val baos = new ByteArrayOutputStream()
	  val tempOut = new DataOutputStream(baos) 
	  val out = new EncryptedRandomAccessFileImpl(file)
    def copyTempOutToOut() = {
      tempOut.flush()
      val bytes = baos.toByteArray()
      out.write(bytes)
      bytes
    }
    def writeEncryptionInfo() {
  	    tempOut.writeByte(encryptionInfo.algorithm)
  	    tempOut.writeByte(encryptionInfo.macAlgorithm)
  	    tempOut.writeByte(encryptionInfo.ivLength)
  	    tempOut.write(encryptionInfo.iv)
    }
    def startEncryptedPart(key: Array[Byte]) {
        val array = copyTempOutToOut()
  	    val keyInfo = new KeyInfo(key)
  	    keyInfo.iv = encryptionInfo.iv
  	    out.setEncryptionBoundary(keyInfo)
  	    out.write(CryptoUtils.hmac(array, keyInfo))
    }
    def writeKeyDerivationInfo() {
  	    tempOut.writeByte(keyDerivationInfo.algorithm)
  	    tempOut.writeByte(keyDerivationInfo.iterationsPower)
  	    tempOut.writeByte(keyDerivationInfo.memoryFactor)
  	    tempOut.writeByte(keyDerivationInfo.keyLength)
  	    tempOut.writeByte(keyDerivationInfo.saltLength)
  	    tempOut.write(keyDerivationInfo.salt)
    }
	  tempOut.write("KvStore".getBytes())
	  // version
	  tempOut.writeByte(0)
	  // type
	  (passphrase, key) match {
	    case (null, null) => // Type 0
  	    tempOut.writeByte(0)
  	    copyTempOutToOut()
	    case (null, key) => // Type 1
  	    tempOut.writeByte(1)
  	    writeEncryptionInfo()
  	    startEncryptedPart(key)
 	    case (pass, null) => // Type 2
  	    tempOut.writeByte(2)
  	    writeKeyDerivationInfo()
  	    val keyHere = CryptoUtils.keyDerive(passphrase, keyDerivationInfo.salt, 
  	         keyDerivationInfo.keyLength, keyDerivationInfo.iterationsPower, keyDerivationInfo.memoryFactor)
  	    writeEncryptionInfo()
  	    startEncryptedPart(keyHere)
	    case (pass, key) => throw new IllegalArgumentException("Just supply either key or passphrase")
	  }
	  out
	}
	
	var lastSync = 0L
	
	def writeEntry(entry: Entry) {
    writer.writeByte(entry.typ.markerByte)
    for (part <- entry.parts) {
      writeEntryPart(part)
    }
    if (writer.getFileLength() - lastSync > fsyncThreshold) { 
      writer.fsync()
      lastSync = writer.getFileLength()
	  }
  }

  def checkpoint(): Unit = writer.fsync()

	def writeEntryPart(part: EntryPart) {
	  writer.writeByte(if (part.hasCrc) 1 else 0)
	  writer.writeVLong(part.array.length)
	  writer.write(part.array)
	  if (part.hasCrc)
	    writer.writeInt(CrcUtil.crc(part.array))
	}
	
  def close() {
    writer.writeByte(255.toByte)
    writer.close()
  }
  def length() = writer.getFileLength()
}

class KvStoreReaderImpl(val file: File, val passphrase: String = null, val keyGiven: Array[Byte] = null) extends KvStoreReader with Utils {
  val maxSupportedVersion = 0
  private var startOfEntries = 0L
  private var encryptionInfo: EncryptionInfo = null
  var _reader: EncryptedRandomAccessFileImpl = null
  def reader() = {
    if (_reader == null) {
      _reader = new EncryptedRandomAccessFileImpl(file)
      val marker = Array.ofDim[Byte](7)
      _reader.read(marker)
      if (!new String(marker).equals("KvStore")) {
        throw new IllegalStateException("File is not a valid kvstore file")
      }
      val version = _reader.readByte()
      if (version > maxSupportedVersion) {
        throw new IllegalStateException("This file was written with an incompatible version")
      }
      def readEncryptionInfo(key: Array[Byte]) {
        val algorithm = _reader.readByte()
        if (algorithm != 0) {
          throw new IllegalArgumentException(s"algorithm $algorithm is not implemented")
        }
        val macAlgorithm = _reader.readByte()
        if (macAlgorithm != 0) {
          throw new IllegalArgumentException(s"macAlgorithm $macAlgorithm is not implemented")
        }
        val ivLength = _reader.readVLong().toByte
        val iv = _reader.readBytes(ivLength)
        encryptionInfo = new EncryptionInfo(algorithm, macAlgorithm, ivLength, iv)
        val keyInfo = new KeyInfo(key)
        keyInfo.iv = encryptionInfo.iv
        val backup = _reader.getFilePos()
        _reader.setEncryptionBoundary(keyInfo)
        val hmacInFile = _reader.readBytes(32)
        _reader.seek(0)
        val bytesToHmac = _reader.readBytes(backup.toInt)
        val hmacComputed = CryptoUtils.hmac(bytesToHmac, keyInfo)
        if (!Arrays.equals(hmacInFile, hmacComputed)) {
          throw new IllegalStateException("Hmac verification failed")
        }
        _reader.skip(32)
      }
      val filetype = _reader.readByte()
      filetype match {
        case 0 => // unencrypted file, we are done
        case 1 => // initialize encryption
          readEncryptionInfo(keyGiven)
        case 2 => // file with passphrase
          val algorithm = _reader.readByte()
          if (algorithm != 0) {
            throw new IllegalArgumentException(s"algorihtm $algorithm is not implemented")
          }
          val iterationsPower = _reader.readByte()
          val memoryFactor = _reader.readByte()
          val keyLength = _reader.readByte()
          val saltLength = _reader.readByte()
          val salt = _reader.readBytes(saltLength)
          val key = CryptoUtils.keyDerive(passphrase, salt, keyLength, iterationsPower, memoryFactor)
          readEncryptionInfo(key)
        case e => throw new IllegalArgumentException(s"Filetype $e is not implemented")
      }
      startOfEntries = _reader.getFilePos()
    }
    _reader
  }

  def iterator() = {
    new Iterator[Entry]() {
      var pos = getPosOfFirstEntry()
      def hasNext = pos < file.length
      def next = {
        val out = readEntryAt(pos).get
        pos = out.getEndOfEntryPos()
        out
      }
    }
  }

  def checkAndFixFile(): Boolean = {
    var success = true
    try {
      reader.getFilePos()
    } catch {
      case e: Exception =>
        l.error(s"Got exception while opening kvstore $file, deleting whole file", e)
        if (reader != null)
          reader.raf.close()
        reader.file.delete()
        return false
    }
    val backup = reader.getFilePos()
    var continue = true
    val it = iterator()
    var lastGoodPos = getPosOfFirstEntry()
    var seenEndMarker = false
    while (it.hasNext && continue) {
      var delete = false
      var e: Entry = null
      try {
        e = it.next
        if (e.typ == EntryTypes.endOfFile) {
          seenEndMarker = true
        }
      } catch {
        case e: Exception =>
          l.warn(s"Exception ${e.getMessage} happened while trying to read entry at offset $lastGoodPos in file $file")
        delete = true
      }
      if (e != null && e.getEndOfEntryPos() > reader.getFileLength()) {
        l.warn(s"Entry at $lastGoodPos is longer than file $file")
        delete = true
      } else if (e != null) {
        lastGoodPos = e.getEndOfEntryPos()
      }
      if (e == null || delete) {
        continue = false
        try {
          reader.seek(lastGoodPos)
          reader.writeByte(255.toByte)
          reader.truncateRestOfFile()
          seenEndMarker = true
          l.warn(s"Truncated file $file to $lastGoodPos bytes")
        } catch {
          case e: Exception => l.error(s"Exception while fixing $file", e)
            success = false
        }
      }
    }
    if (!seenEndMarker) {
      l.warn(s"File $file has not been closed properly")
      reader.writeByte(255.toByte)
      reader.fsync()
    }
    reader.seek(backup)
    success
  }
  
  def close(): Unit = {
    reader.close()
  }
  
  def getPosOfFirstEntry() = {
    reader.getFileLength()
    startOfEntries
  }
  
  def readEntryPartAt(pos: Long): EntryPart = {
    reader.seek(pos)
    val entryType = reader.readByte()
    entryType match {
      case 0 => readPlainValue(pos, false)
      case 1 => readPlainValue(pos, true)
      case e => throw new IllegalArgumentException("Expected type of entrypart to be 0, was "+e)
    }
  }
  
  def readPlainValue(startPosOfEntry: Long, hasCrcIn: Boolean) = {
    val length = reader.readVLong()
    val offset = if (hasCrcIn) 4 else 0
    new EntryPart() {
      private val startOfValue = reader.getFilePos()
      val startPos = startPosOfEntry
      lazy val array = {
        reader.seek(startOfValue)
        val array = Array.ofDim[Byte](length.toInt)
        reader.read(array)
        if (hasCrcIn) {
          val crc = reader.readInt()
          CrcUtil.checkCrc(array, crc, "Crc failed while reading plain value at "+startPosOfEntry)
        }
        array
      }
      val hasCrc = hasCrcIn
      val endPos = startOfValue + length + offset 
      reader.seek(endPos)
    }
  }
  
  def readEntryAt(pos: Long): Option[Entry] = {
    if (pos >= reader.getFileLength()) {
      return None
    }
    reader.seek(pos)
    val typ = EntryTypes.getTypeForByte(reader.readByte())
    typ match {
      case EntryTypes.keyValue =>
        val key = readEntryPartAt(reader.getFilePos())
        val value = readEntryPartAt(reader.getFilePos())
        Some(new Entry(typ, key::value::Nil, pos))
      case EntryTypes.endOfFile =>
        Some(new Entry(typ, Nil, pos))
      case _ =>
        throw new IllegalArgumentException("Expected key value entry type at "+pos)
    }
  }
  
}