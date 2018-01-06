package ch.descabato.core.kvstore

import java.io.{File, RandomAccessFile}
import java.nio.ByteBuffer
import java.security.Security
import java.util
import javax.crypto.Cipher

import ch.descabato.akka.ActorStats
import ch.descabato.core.BlockingOperation
import ch.descabato.utils.{BytesWrapper, Utils}
import org.bouncycastle.jce.provider.BouncyCastleProvider

import scala.collection.immutable.TreeMap
import scala.concurrent.{ExecutionContext, _}
import scala.concurrent.duration._

class KeyInfo(val key: Array[Byte]) {
  def ivSize = 16
  var iv: Array[Byte] = _
}

trait RandomAccessFileUser extends AutoCloseable {
  lazy val raf = new RandomAccessFile(file, mode)
  def mode: String
  def file: File
  def getFileLength(): Long = raf.length()
  def close(): Unit = raf.close()
  def seek(pos: Long): Unit = raf.seek(pos)
  def skip(skip: Long): Unit = raf.seek(raf.getFilePointer()+skip)
  def getFilePos(): Long = raf.getFilePointer()
}

trait EncryptedRandomAccessFile extends AutoCloseable {
  def seek(pos: Long)
  def skip(pos: Long)
  def setEncryptionBoundary(keyInfo: KeyInfo)

  final def write(bytes: BytesWrapper): Unit = {
    writeImpl(bytes.array, bytes.offset, bytes.length)
  }

  def writeImpl(bytes: Array[Byte], offset: Int, len: Int)
  final def read(bytes: Array[Byte], offset: Int = 0, len: Int = -1): Unit = {
    readImpl(bytes, offset, if (len == -1) bytes.length else len)
  }
  def readImpl(bytes: Array[Byte], offset: Int, len: Int)

  def fsync(): BlockingOperation
}

trait EncryptedRandomAccessFileHelpers extends EncryptedRandomAccessFile with RandomAccessFileUser {

  def writeInt(value: Int): Unit = {
    val buffer = ByteBuffer.allocate(4).putInt(value)
    val intArray = buffer.array()
    writeImpl(intArray, 0, 4)
  }

  def writeByte(arg: Byte): Unit = {
    val byte = Array.apply(arg)
    writeImpl(byte, 0, 1)
  }

  def writeVLong(value: Long) {
    ZigZag.writeLong(value, this)
  }

  def readVLong(): Long = {
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

  def readBytes(len: Int): Array[Byte] = {
    val array = Array.ofDim[Byte](len)
    read(array, 0, len)
    array
  }

}

class EncryptedRandomAccessFileImpl(val file: File, val readOnly: Boolean = false) extends EncryptedRandomAccessFileHelpers with Utils {
  implicit val context: ExecutionContextExecutor = ExecutionContext.fromExecutor(ActorStats.tpe)

  val mode: String = if (readOnly) "r" else "rw"

  var futureFailed = 0
  var futureWorked = 0
  var futuresWaitedFor = 0L
  var waitedForFutures = 0L

  class Block(val blockNum: Int, val size: Int) {

    val startOfBlockPos: Long = blockNum * cipher.getBlockSize + encryptedFrom
    var _cipherBytes: Array[Byte] = _
    var _plainBytes: Array[Byte] = _
    var _future: Future[(Array[Byte], Array[Byte])] = _
    var _diskCipherSynced = true
    var _plainCipherSynced = true
    var endOfBlock: Int = Math.min(size, Math.max(currentPos, getFileLength()) - startOfBlockPos).toInt

    def cipherBytes(): Array[Byte] = {
      if (_cipherBytes == null && raf.length() >= startOfBlockPos) {
        raf.seek(startOfBlockPos)
        _cipherBytes = Array.ofDim[Byte](endOfBlock)
        val validBytes = raf.read(_cipherBytes)
        if (validBytes < _cipherBytes.length) {
          _cipherBytes = _cipherBytes.take(validBytes)
        }
      }
      _cipherBytes
    }

    def plainBytes(): Array[Byte] = {
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

    def spawnFuture() {
      val copy = util.Arrays.copyOf(_plainBytes, _plainBytes.length)
      val endHere = endOfBlock
      _future = Future {
        try {
          encryptBytes(copy, endHere)
        } catch {
          case e: Exception =>
            l.warn("Future evaluation failed ", e)
            (null, null)
        }
      }
    }

    def updateBytes(x: Array[Byte], xOffset: Int, filePos: Long, len: Int): Int = {
      val destOffset = (filePos - startOfBlockPos).toInt
      val lenHere = List(len, size - destOffset).min
      plainBytes()
      l.trace(s"Block starting at $startOfBlockPos destOffset is $destOffset, lenHere $lenHere, xOffset $xOffset, length ${_plainBytes.length}, endOfBlock $endOfBlock")
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

      if (destOffset + lenHere == size) {
        spawnFuture()
      }
      lenHere
    }

    def readBytes(bytes: Array[Byte], destOffset: Int, filePos: Long, len: Int): Int = {
      val srcOffset = (filePos - startOfBlockPos).toInt
      val lenHere = List(len, endOfBlock - srcOffset).min
      l.trace(s"Copying $lenHere bytes from plainBytes (${plainBytes().length} at $srcOffset) to bytes (${bytes.length} at $destOffset), at pos $filePos/${raf.length()}")
      System.arraycopy(plainBytes(), srcOffset, bytes, destOffset, lenHere)
      lenHere
    }

    def truncateTo(filePos: Long): Unit = {
      endOfBlock = (filePos - startOfBlockPos).toInt
      l.trace(s"Block starting at $startOfBlockPos truncating to $filePos, end of block now $endOfBlock")
      _future = null
      _plainCipherSynced = false
      _diskCipherSynced = false
    }

    def encryptBytes(plainBytes: Array[Byte], endHere: Int): (Array[Byte], Array[Byte]) = {
      val iv = CryptoUtils.deriveIv(keyInfo.iv, blockNum)
      val cipherHere = Cipher.getInstance("AES/CTR/NoPadding", "BC")
      cipherHere.init(Cipher.ENCRYPT_MODE, CryptoUtils.keySpec(keyInfo), iv)
      l.trace(s"Encrypting block $startOfBlockPos to write again with length $endHere")
      val cipherBytes = cipherHere.doFinal(plainBytes, 0, endHere)
      (cipherBytes, plainBytes)
    }

    def write(): Unit = {
      l.trace(s"Writing block at $startOfBlockPos, synced? ${_plainCipherSynced}")
      if (!_plainCipherSynced) {
        var result: (Array[Byte], Array[Byte]) = null
        if (_future != null) {
          val startedWaiting = System.currentTimeMillis()
          result = Await.result(_future, 10.minutes)
          futuresWaitedFor += 1
          waitedForFutures += System.currentTimeMillis() - startedWaiting
        }
        if (result == null || result._1 == null || !java.util.Arrays.equals(result._2, _plainBytes)) {
          result = encryptBytes(_plainBytes, endOfBlock)
        }
        _cipherBytes = result._1
        _plainCipherSynced = true
      }
      if (!_diskCipherSynced) {
        raf.seek(startOfBlockPos)
        raf.write(_cipherBytes)
        _diskCipherSynced = true
      }
    }
  }

  class BlockCache(val maxMemory: Int = 5*1024*1024) {
    private var cache = TreeMap[Int, Block]()
    var currentSize = 0

    def dropEntriesRightOf(i: Int) {
      cache = cache.to(i)
      recalcCurrentSize()
    }

    def recalcCurrentSize() {
      currentSize = cache.map(_._2.size).sum
    }

    def getBlock(length: Int = 0): Block = {
      val encryptedStreamPos = currentPos - encryptedFrom
      val startBlock = (encryptedStreamPos / blockSize).toInt
      val partialTreeMap = cache.to(startBlock)
      val option = partialTreeMap.lastOption.filter {
        case (num, b) =>
          b.startOfBlockPos + b.size > currentPos
      }
      if (option.isDefined) {
        return option.get._2
      } else {
        var len = length >> 5
        len += 1
        val blockSize = len << 5
        cache += startBlock -> new Block(startBlock, blockSize)
        currentSize += blockSize
        while (currentSize > maxMemory) {
          l.trace(s"Removing an entry from cache because $currentSize > $maxMemory")
          val option = (cache - startBlock).headOption
          option.foreach { case (k, v) =>
            l.trace(s"Removed entry is ${v.blockNum} with size ${v.size}")
            v.write()
            cache = cache - k
            currentSize -= v.size
          }
        }
      }
      cache(startBlock)
    }

    def flushWrites() {
      cache.valuesIterator.foreach(_.write())
    }

    def getLastPos(): Long = cache.lastOption.map{ case (_, b) => b.endOfBlock + b.startOfBlockPos}.getOrElse(0L)

    def clear(): Unit = {
      currentSize = 0
      cache = TreeMap.empty
    }
  }

  var encryptedFrom: Long = Long.MaxValue

  var noException = true
  var keyInfo: KeyInfo = _
  var cipher: Cipher = _
  var _currentPos = 0L
  def currentPos: Long = {
    l.trace("Getting currentpos") // "+Thread.currentThread().getStackTrace.mkString("\n"))
    _currentPos
  }
  def currentPos_=(newValue: Long) {
    l.trace(s"Setting currentpos to $newValue" ) //+Thread.currentThread().getStackTrace.mkString("\n"))
    _currentPos = newValue
  }

  def blockSize: Int = cipher.getBlockSize

  val blockCache = new BlockCache()

  override def fsync(): BlockingOperation = {
    if (raf.getChannel.isOpen) {
      blockCache.flushWrites()
      raf.getChannel.force(false)
    } else {
      throw new IllegalStateException(s"Can not fsync because channel is closed for file $file")
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
        throw new IllegalArgumentException(s"May not read or write across encryption boundary of file $file")
      }
    }
    if (read && getFilePos() + len > getFileLength()) {
      noException = false
      throw new IllegalArgumentException(s"May not read after end ($len bytes from ${getFilePos()}) of file $file with length ${getFileLength()}")
    }
  }

  override def writeImpl(bytes: Array[Byte], offset: Int, len: Int) {
    l.trace(s"Writing bytes from $file from $offset to $len")
    boundaryCheck(len, read = false)
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
        val block = blockCache.getBlock(bytesLeft)
        val writingNow = block.updateBytes(bytes, offsetNow, currentPos, bytesLeft)
        bytesLeft -= writingNow
        offsetNow += writingNow
        currentPos += writingNow
      }
    }
  }

  override def readImpl(bytes: Array[Byte], offset: Int, len: Int) {
    l.trace(s"Reading bytes from $file from $offset to $len")
    boundaryCheck(len, read = true)
    raf.seek(currentPos)
    if (getFilePos() < encryptedFrom) {
      raf.read(bytes, offset, len)
      currentPos += len
    } else {
      var bytesLeft = len
      var offsetNow = 0
      while (bytesLeft > 0) {
        val readingNow = blockCache.getBlock(bytesLeft).readBytes(bytes, offsetNow, currentPos, bytesLeft)
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
      blockCache.clear()
    }
    if (futuresWaitedFor != 0) {
      l.info(s"Waited for $futuresWaitedFor a total of $waitedForFutures")
    }
    raf.close()
  }

  override def seek(pos: Long): Unit = {
    currentPos = pos
  }

  override def skip(pos: Long): Unit = {
    currentPos = currentPos + pos
  }

  override def getFilePos(): Long = currentPos

  override def getFileLength(): Long = {
    val o = blockCache.getLastPos()
    List(super.getFileLength(), o).max
  }

  def truncateRestOfFile(): BlockingOperation = {
    raf.setLength(currentPos)
    if (encryptedFrom <= currentPos) {
      val block = blockCache.getBlock()
      block.truncateTo(currentPos)
      blockCache.dropEntriesRightOf(block.blockNum)
    }
    fsync()
  }
}
