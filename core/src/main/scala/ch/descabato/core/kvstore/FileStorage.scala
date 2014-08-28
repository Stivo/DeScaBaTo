package ch.descabato.core.kvstore

import java.io.File
import java.io.RandomAccessFile
import java.nio.ByteBuffer
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

trait FileWriter extends AutoCloseable {
  
  def write(bytes: Array[Byte], start: Int = 0, end: Int = -1): Unit
  
  def getFileLength(): Long
  
  def writeLong(value: Long) = {
    val buffer = ByteBuffer.allocate(8).putLong(value)
    val longArray = buffer.array()
    write(longArray)
  }
  
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
 
   def fsync(): Unit
}

trait EncryptedFileWriter extends FileWriter {
  def turnEncryptionOn(keyInfo: KeyInfo): Unit  
}

trait FileReader extends AutoCloseable {
  
  def seek(pos: Long): Unit
  def skip(pos: Long): Unit
  def getFilePos(): Long
  final def read(bytes: Array[Byte], start: Int = 0, end: Int = -1): Unit = {
    val endHere = if (end <= 0) bytes.length else end
    if (endHere == start)
      return
    readImpl(bytes, start, endHere)
  }

  final def readBytes(length: Int) = {
    val array = Array.ofDim[Byte](length)
    readImpl(array, 0, length)
    array
  }

  protected def readImpl(bytes: Array[Byte], start: Int, end: Int): Unit
  
  def getFileLength(): Long
  
  def readVLong() = {
	  ZigZag.readLong(this)
  }
  
  def readLong(): Long = {
    val longArray = Array.ofDim[Byte](8)
    read(longArray)
    ByteBuffer.wrap(longArray).asLongBuffer().get()
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
}

trait EncryptedFileReader extends FileReader {
  def turnEncryptionOn(pos: Long, keyInfo: KeyInfo): Unit  
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

class PlainFileWriter(val file: File) extends FileWriter with RandomAccessFileUser {
  val mode = "w" 
  def write(bytes: Array[Byte], start: Int = 0, end: Int = -1) {
    val endHere = if (end == -1) bytes.length else end
    raf.write(bytes, start, endHere)
  }

  def fsync(): Unit = {
    raf.getChannel().force(false)
  }
}

class PlainFileReader(val file: File) extends FileReader with RandomAccessFileUser {
  val mode = "r"
  def readImpl(bytes: Array[Byte], start: Int, end: Int): Unit = {
    raf.read(bytes, start, end)
  }
  
}

class KeyInfo(val key: Array[Byte]) {
  def ivSize = 16
  var iv: Array[Byte] = null
}

class EncryptedFileWriterAes(val file: File) extends EncryptedFileWriter {
  private var encryptionOn = false
  
  Security.addProvider(new BouncyCastleProvider());
  private val cipher = Cipher.getInstance("AES/CTR/NoPadding", "BC");
  
  lazy private val outputStream = new FileOutputStream(file);
  lazy private val cipherOutputStream = new CipherOutputStream(outputStream, cipher);
  lazy private val dataOutputStream = new DataOutputStream(cipherOutputStream)
  //lazy private val bufferedDataOutputStream = new BufferedOutputStream(dataOutputStream)

  private var writtenBytes = 0L
  
  def turnEncryptionOn(keyInfo: KeyInfo) {
//    val ivBytes = Array.ofDim[Byte](keyInfo.ivSize)
//    SecureRandom.getInstanceStrong().nextBytes(ivBytes)
//    write(ivBytes)
//    keyInfo.iv = ivBytes
    val iv = new IvParameterSpec(keyInfo.iv)
    cipher.init(Cipher.ENCRYPT_MODE, CryptoUtils.keySpec(keyInfo), iv)
    encryptionOn = true
  }
  
  def write(bytes: Array[Byte], start: Int, end: Int): Unit = {
    val endHere = if (end == -1) bytes.length else end
    writtenBytes += endHere - start
    if (encryptionOn) {
    	dataOutputStream.write(bytes, start, endHere)
    } else {
      outputStream.write(bytes, start, endHere)
    }
  }

  def close(): Unit = {
    if (encryptionOn) {
      dataOutputStream.close()
    } else {
      outputStream.close()
    }
  }

  def getFileLength() = writtenBytes

  def fsync(): Unit = {
    if (encryptionOn) {
      dataOutputStream.flush()
    }
    outputStream.getChannel().force(false)
  }

}

class EncryptedFileReaderAes(val file: File) extends EncryptedFileReader with RandomAccessFileUser {
  var cacheHits = 0
  var requests = 0
  val mode = "r"
  var encryptedFrom = getFileLength()
  var keyInfo: KeyInfo = null
  
  var decryptedPartCache: Option[(Long, Array[Byte])] = None
  
  Security.addProvider(new BouncyCastleProvider());
  val cipher = Cipher.getInstance("AES/CTR/NoPadding", "BC");

  def turnEncryptionOn(pos: Long, keyInfo: KeyInfo) {
    encryptedFrom = pos
    this.keyInfo = keyInfo
    cipher.init(Cipher.DECRYPT_MODE, CryptoUtils.keySpec(keyInfo), CryptoUtils.deriveIv(keyInfo.iv, 0))
  }

  def readImpl(bytes: Array[Byte], start: Int, end: Int) {
    requests += 1
    val length = end - start
    val filePos = raf.getFilePointer()
    if (filePos + length > getFileLength()) {
      throw new IllegalArgumentException("Reading past end of file")
    }
    if (filePos < encryptedFrom) {
      if (filePos + length > encryptedFrom) {
        throw new IllegalArgumentException("ranges over unencrypted and encrypted parts of file")
      }
      raf.read(bytes, start, end)
    } else {
    	var blockSize = cipher.getBlockSize()
    	
    	val encryptedStreamPos = filePos - encryptedFrom
    	// find involved blocks
   	  val startBlock = (encryptedStreamPos / blockSize).toInt
  	  val endBlock = ((encryptedStreamPos + length + blockSize - 1) / blockSize).toInt
  	  val totalLength = (endBlock - startBlock) * blockSize
      val offsetInBlock = (encryptedStreamPos % cipher.getBlockSize()).toInt
      val fileOffsetOfBlock = filePos-offsetInBlock
      // read bytes for these blocks
   	  val decryptedBlock = decryptedPartCache match {
    	  case Some((l, ar)) if (l == fileOffsetOfBlock && length+offsetInBlock <= ar.length) =>
    	    cacheHits += 1
    	    ar
    	  case _ =>
          cipher.init(Cipher.DECRYPT_MODE, CryptoUtils.keySpec(keyInfo), CryptoUtils.deriveIv(keyInfo.iv, startBlock))
          raf.seek(filePos-offsetInBlock)
          val encryptedBlock = Array.ofDim[Byte](totalLength)
          raf.read(encryptedBlock)
          // decrypt bytes
          cipher.update(encryptedBlock, 0, encryptedBlock.length, encryptedBlock)
          decryptedPartCache = Some((fileOffsetOfBlock, encryptedBlock))
          encryptedBlock
    	}

      // copy over demanded bytes to dest
      System.arraycopy(decryptedBlock, offsetInBlock, bytes, start, end-start)
    }
    raf.seek(filePos + length)
  }

}

/**
 * Zig-zag encoder used to write object sizes to serialization streams.
 * Based on Kryo's integer encoder.
 */
object ZigZag {
  def writeLong(n: Long, out: FileWriter) {
    var value = n
    while((value & ~0x7F) != 0) {
      out.writeByte(((value & 0x7F) | 0x80).toByte)
      value >>>= 7
    }
    out.writeByte(value.toByte)
  }

  def readLong(in: FileReader): Long = {
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
