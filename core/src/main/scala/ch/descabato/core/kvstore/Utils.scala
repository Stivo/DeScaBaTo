package ch.descabato.core.kvstore

import javax.crypto.spec.IvParameterSpec
import java.math.BigInteger
import javax.crypto.spec.SecretKeySpec
import java.security.SecureRandom
import javax.crypto.Mac
import ch.descabato.utils.Utils
import org.bouncycastle.crypto.generators.SCrypt
import java.util.zip.CRC32

object CryptoUtils extends Utils {
  def deriveIv(iv: Array[Byte], offset: Int) = {
    var bigInt = new BigInteger(iv)
    bigInt = bigInt.add(new BigInteger(offset+""))
    var bytes = bigInt.toByteArray()
    if (bytes.length != iv.length) {
      while (bytes.length < iv.length) {
        bytes = Array(0.toByte) ++ bytes
      }
      while (bytes.length > iv.length) {
        bytes = bytes.drop(1)
      }
    }
    new IvParameterSpec(bytes)
  }
  def newStrongRandomByteArray(ivLength: Short) = {
    val iv = Array.ofDim[Byte](ivLength)
    SecureRandom.getInstance("PKCS11").nextBytes(iv)
    iv
  }
  
  def keySpec(keyInfo: KeyInfo) = new SecretKeySpec(keyInfo.key, "AES")
  
  def hmac(bytes: Array[Byte], keyInfo: KeyInfo) = {
     newHmac(keyInfo).doFinal(bytes)
  }
  
  def newHmac(keyInfo: KeyInfo) = {
     val sha256_HMAC = Mac.getInstance("HmacSHA256")
     val secret_key = new SecretKeySpec(keyInfo.key, "HmacSHA256")
     sha256_HMAC.init(secret_key)
     sha256_HMAC
  }
  
  implicit class PowerInt(val i:Int) extends AnyVal {
    def ** (exp:Int):Int = Math.pow(i,exp).toInt
  }
  
  def keyDerive(passphrase: String, salt: Array[Byte], keyLength: Byte = 16, iterationsPower: Int = 16, memoryFactor: Int = 8) = {
    SCrypt.generate(passphrase.getBytes("UTF-8"), salt, 2**iterationsPower, memoryFactor, 4, keyLength)
  }
  
}

object CrcUtil {
  def crc(bytes: Array[Byte]) = {
    val crc = new CRC32()
    crc.update(bytes)
    crc.getValue().toInt
  }
  def checkCrc(bytes: Array[Byte], expected: Int, m: String = "") = {
    if (crc(bytes) != expected) {
      throw new IllegalArgumentException("Crc check failed: "+m)
    }
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


