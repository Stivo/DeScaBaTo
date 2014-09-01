package ch.descabato.core.kvstore

import java.io.File
import java.io.RandomAccessFile
import java.nio.ByteBuffer
import ch.descabato.core.BlockingOperation
import org.bouncycastle.jce.provider.BouncyCastleProvider
import java.security.Security
import javax.crypto.Cipher
import javax.crypto.spec.IvParameterSpec
import javax.crypto.spec.SecretKeySpec
import java.io.FileOutputStream
import java.io.BufferedOutputStream
import javax.crypto.CipherOutputStream
import java.io.DataOutputStream
import java.security.SecureRandom
import java.io.EOFException
import ch.descabato.utils.Implicits._
import ch.descabato.utils.Utils
import scala.collection.mutable

import scala.collection.immutable.{TreeMap, HashMap}

class KeyInfo(val key: Array[Byte]) {
  def ivSize = 16
  var iv: Array[Byte] = null
}

trait RandomAccessFileUser extends AutoCloseable {
  lazy val raf = new RandomAccessFile(file, mode)
  def mode: String
  def file: File
  def getFileLength() = raf.length()
  def close() = raf.close
  def seek(pos: Long) = raf.seek(pos)
  def skip(skip: Long) = raf.seek(raf.getFilePointer()+skip)
  def getFilePos() = raf.getFilePointer()
}

trait EncryptedRandomAccessFile extends AutoCloseable {
  def seek(pos: Long)
  def skip(pos: Long)
  def setEncryptionBoundary(keyInfo: KeyInfo)
  final def write(bytes: Array[Byte], offset: Int = 0, len: Int = -1) = {
    writeImpl(bytes, offset, if (len == -1) bytes.length else len)
  }
  def writeImpl(bytes: Array[Byte], offset: Int, len: Int)
  final def read(bytes: Array[Byte], offset: Int = 0, len: Int = -1) = {
    readImpl(bytes, offset, if (len == -1) bytes.length else len)
  }
  def readImpl(bytes: Array[Byte], offset: Int, len: Int)

  def fsync(): BlockingOperation
}

trait EncryptedRandomAccessFileHelpers extends EncryptedRandomAccessFile with RandomAccessFileUser {
  val mode = "rw"
  def writeInt(value: Int) = {
    val buffer = ByteBuffer.allocate(4).putInt(value)
    val intArray = buffer.array()
    write(intArray)
  }

  def writeByte(arg: Byte) = {
    val byte = Array.apply(arg)
    write(byte)
  }

  def writeVLong(value: Long) {
    ZigZag.writeLong(value, this)
  }

  def readVLong() = {
    ZigZag.readLong(this)
  }

  def readInt(): Int = {
    val intArray = Array.ofDim[Byte](4)
    read(intArray)
    ByteBuffer.wrap(intArray).asIntBuffer().get()
  }

  def readByte(): Byte = {
    val intArray = Array.ofDim[Byte](1)
    read(intArray)
    intArray(0)
  }

  def readBytes(len: Int) = {
    val array = Array.ofDim[Byte](len)
    read(array, 0, len)
    array
  }

}

class EncryptedRandomAccessFileImpl(val file: File) extends EncryptedRandomAccessFileHelpers with Utils {
  var encryptedFrom = Long.MaxValue

  var noException = true
  var keyInfo: KeyInfo = null
  var cipher: Cipher = null
  var _currentPos = 0L
  def currentPos = {
    l.trace("Getting currentpos") // "+Thread.currentThread().getStackTrace.mkString("\n"))
    _currentPos
  }
  def currentPos_=(newValue: Long) {
    l.trace(s"Setting currentpos to $newValue" ) //+Thread.currentThread().getStackTrace.mkString("\n"))
    _currentPos = newValue
  }

  def blockSize = cipher.getBlockSize
  class Block(val blockNum: Int, val size: Int) {

    val startOfBlockPos = blockNum * cipher.getBlockSize + encryptedFrom
    var _cipherBytes: Array[Byte] = null
    var _plainBytes: Array[Byte] = null
    var _diskCipherSynced = true
    var _plainCipherSynced = true
    var endOfBlock = Math.min(size, Math.max(currentPos, getFileLength()) - startOfBlockPos).toInt
    def cipherBytes() = {
      if (_cipherBytes == null && raf.length() >= startOfBlockPos + endOfBlock) {
        raf.seek(startOfBlockPos)
        _cipherBytes = Array.ofDim[Byte](endOfBlock)
        raf.read(_cipherBytes)
      }
      _cipherBytes
    }
    def plainBytes() = {
      if (_plainBytes == null) {
        val cipherBytes2 = cipherBytes()
        if (cipherBytes2 != null) {
          val iv = CryptoUtils.deriveIv(keyInfo.iv, blockNum)
          cipher.init(Cipher.DECRYPT_MODE, CryptoUtils.keySpec(keyInfo), iv)
          _plainBytes = cipher.doFinal(cipherBytes2)
        } else {
          _plainBytes = Array.ofDim[Byte](0)
        }
      }
      _plainBytes
    }
    def updateBytes(x: Array[Byte], xOffset: Int, filePos: Long, len: Int) = {
      val destOffset = (filePos - startOfBlockPos).toInt
      val lenHere = List(len, size - destOffset).min
      plainBytes()
//      System.out.println(s"destOffset is $destOffset, lenHere $lenHere, xOffset $xOffset, length ${_plainBytes.length}")
      val backup = _plainBytes
      if (_plainBytes.length < size) {
        _plainBytes = Array.ofDim[Byte](size)
        if (backup.length > 0) {
          System.arraycopy(backup, 0, _plainBytes, 0, backup.length)
        }
      }
      System.arraycopy(x, xOffset, _plainBytes, destOffset, lenHere)
      endOfBlock = Math.max(destOffset + lenHere, endOfBlock)
      _plainCipherSynced = false
      _diskCipherSynced = false
      lenHere
    }
    def readBytes(bytes: Array[Byte], destOffset: Int, filePos: Long, len: Int) = {
      val srcOffset = (filePos - startOfBlockPos).toInt
      val lenHere = List(len, endOfBlock - srcOffset).min
      //System.out.println(s"Copying $lenHere bytes from plainBytes (${plainBytes().length} at $srcOffset) to bytes (${bytes.length} at $destOffset), at pos $filePos/${raf.length()}")
      System.arraycopy(plainBytes(), srcOffset, bytes, destOffset, lenHere)
      lenHere
    }
    def truncateTo(filePos: Long) = {
      endOfBlock = (filePos - startOfBlockPos).toInt
      _plainCipherSynced = false
      _diskCipherSynced = false
    }
    def write(): Unit = {
      if (!_plainCipherSynced) {
        val iv = CryptoUtils.deriveIv(keyInfo.iv, blockNum)
        cipher.init(Cipher.ENCRYPT_MODE, CryptoUtils.keySpec(keyInfo), iv)
        _cipherBytes = cipher.doFinal(_plainBytes, 0, endOfBlock)
        _plainCipherSynced = true
      }
      if (!_diskCipherSynced) {
        raf.seek(startOfBlockPos)
        raf.write(_cipherBytes)
        _diskCipherSynced = true
      }
    }
  }

  var blockCache = TreeMap[Int, Block]()

  def getBlock(length: Int = 0): Block = {
    val encryptedStreamPos = currentPos - encryptedFrom
    // find involved blocks
    val startBlock = (encryptedStreamPos / blockSize).toInt
    val partialTreeMap = blockCache.to(startBlock)
    val option = partialTreeMap.lastOption.filter {
      case (num, b) =>
        b.startOfBlockPos + b.size > currentPos
    }
    if (option.isDefined) {
      return option.get._2
    } else {
      var l = length >> 5
      l += 1
      val blockSize = l << 5
      blockCache += startBlock -> new Block(startBlock, blockSize)
    }
    blockCache(startBlock)
  }

  override def fsync(): BlockingOperation = {
    if (noException) {
      blockCache.values.foreach(_.write())
      blockCache = TreeMap.empty
      raf.getChannel.force(false)
    }
    new BlockingOperation()
  }

  override def setEncryptionBoundary(keyInfo: KeyInfo): Unit = {
    encryptedFrom = getFilePos()
    this.keyInfo = keyInfo
    Security.addProvider(new BouncyCastleProvider())
    cipher = Cipher.getInstance("AES/CTR/NoPadding", "BC")
  }

  private def boundaryCheck(len: Int, read: Boolean) {
    if (getFilePos() < encryptedFrom) {
      if (getFilePos() + len > encryptedFrom) {
        noException = false
        throw new IllegalArgumentException("May not read or write across encryption boundary")
      }
    }
    if (read && getFilePos() + len > getFileLength()) {
      noException = false
      throw new IllegalArgumentException("May not read after end of file")
    }
  }

  override def writeImpl(bytes: Array[Byte], offset: Int, len: Int) {
    l.trace(s"Writing bytes from $file from $offset to $len")
    boundaryCheck(len, false)
    if (raf.getFilePointer != currentPos)
      raf.seek(currentPos)
    if (getFilePos() < encryptedFrom) {
      raf.write(bytes, offset, len)
      currentPos += len
    } else {
      var bytesLeft = len
      var offsetNow = 0
      while (bytesLeft > 0) {
        // load cipher text for block
        val block = getBlock(bytesLeft)
        val writingNow = block.updateBytes(bytes, offsetNow, currentPos, bytesLeft)
        bytesLeft -= writingNow
        offsetNow += writingNow
        currentPos += writingNow
      }
    }
  }

  override def readImpl(bytes: Array[Byte], offset: Int, len: Int) {
    l.trace(s"Reading bytes from $file from $offset to $len")
    boundaryCheck(len, true)
    raf.seek(currentPos)
    if (getFilePos() < encryptedFrom) {
      raf.read(bytes, offset, len)
      currentPos += len
    } else {
      var bytesLeft = len
      var offsetNow = 0
      while (bytesLeft > 0) {
        val readingNow = getBlock(bytesLeft).readBytes(bytes, offsetNow, currentPos, bytesLeft)
        if (readingNow == 0) {
          //l.warn("Did not read any bytes")
          noException = false
          throw new IllegalStateException("Internal problem")
        }
        currentPos += readingNow
        bytesLeft -= readingNow
        offsetNow += readingNow
      }
    }
  }

  override def close() {
    if (noException) {
      fsync()
    }
    raf.close()
  }

  override def seek(pos: Long): Unit = {
    currentPos = pos
  }

  override def skip(pos: Long): Unit = {
    currentPos = currentPos + pos
  }

  override def getFilePos() = currentPos

  override def getFileLength(): Long = {
    val o = blockCache.lastOption.map{ case (_, b) => b.endOfBlock + b.startOfBlockPos}
    (List(super.getFileLength()) ++ o.iterator).max
  }

  def truncateRestOfFile() = {
    raf.setLength(currentPos)
    if (encryptedFrom <= currentPos) {
      val block = getBlock()
      block.truncateTo(currentPos)
      blockCache = blockCache.to(block.blockNum)
    }
    fsync()
  }
}
/**
  * Zig-zag encoder used to write object sizes to serialization streams.
 * Based on Kryo's integer encoder.
 */
object ZigZag {

  def writeLong(n: Long, out: EncryptedRandomAccessFileHelpers) {
    var value = n
    while((value & ~0x7F) != 0) {
      out.writeByte(((value & 0x7F) | 0x80).toByte)
      value >>>= 7
    }
    out.writeByte(value.toByte)
  }

  def readLong(in: EncryptedRandomAccessFileHelpers): Long = {
    var offset = 0
    var result = 0L
    while (offset < 32) {
      val b = in.readByte()
      result |= ((b & 0x7F) << offset)
      if ((b & 0x80) == 0) {
        return result
      }
      offset += 7
    }
    throw new Exception("Malformed zigzag-encoded integer")
  }
}
