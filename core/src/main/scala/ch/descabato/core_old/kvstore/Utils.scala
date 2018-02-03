package ch.descabato.core_old.kvstore

import java.math.BigInteger
import java.security.SecureRandom
import java.util.Base64
import java.util.zip.CRC32
import javax.crypto.Mac
import javax.crypto.spec.{IvParameterSpec, SecretKeySpec}

import ch.descabato.utils.{BytesWrapper, Utils}
import org.bouncycastle.crypto.generators.SCrypt


object CryptoUtils extends Utils {
  def deriveIv(iv: Array[Byte], offset: Int): IvParameterSpec = {
    var bigInt = new BigInteger(iv)
    bigInt = bigInt.add(new BigInteger(offset+""))
    var bytes = bigInt.toByteArray
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
  def newStrongRandomByteArray(ivLength: Short): Array[Byte] = {
    val iv = Array.ofDim[Byte](ivLength)
    new SecureRandom().nextBytes(iv)
    iv
  }
  
  def keySpec(keyInfo: KeyInfo) = new SecretKeySpec(keyInfo.key, "AES")
  
  def hmac(bytes: Array[Byte], keyInfo: KeyInfo): Array[Byte] = {
     newHmac(keyInfo).doFinal(bytes)
  }
  
  def newHmac(keyInfo: KeyInfo): Mac = {
     val sha256_HMAC = Mac.getInstance("HmacSHA256")
     val secret_key = new SecretKeySpec(keyInfo.key, "HmacSHA256")
     sha256_HMAC.init(secret_key)
     sha256_HMAC
  }
  
  implicit class PowerInt(val i:Int) extends AnyVal {
    def ** (exp:Int):Int = Math.pow(i,exp).toInt
  }
  
  def keyDerive(passphrase: String, salt: Array[Byte], keyLength: Byte = 16, iterationsPower: Int = 12, memoryFactor: Int = 8): Array[Byte] = {
    SCrypt.generate(passphrase.getBytes("UTF-8"), salt, 2**iterationsPower, memoryFactor, 4, keyLength)
  }
  
}

object CrcUtil {
  def crc(bytes: BytesWrapper): Int = {
    val crc = new CRC32()
    crc.update(bytes.array, bytes.offset, bytes.length)
    crc.getValue().toInt
  }
  def checkCrc(bytes: BytesWrapper, expected: Int, m: String = ""): Unit = {
    if (crc(bytes) != expected) {
      throw new IllegalArgumentException("Crc check failed: "+m)
    }
  }
}

class EncryptionInfo (
                       // algorithm 0 is for AES/CTR/NoPadding
                       val algorithm: Byte = 0,
                       // algorithm 0 is for HmacSHA256
                       val macAlgorithm: Byte = 0,
                       val ivLength: Byte = 16,
                       val iv: Array[Byte] = CryptoUtils.newStrongRandomByteArray(16)
                     )

class KeyDerivationInfo (
                          // algorithm 0 is for scrypt with 4 14 2 keyLength
                          val algorithm: Byte = 0,
                          val iterationsPower: Byte = 12,
                          val memoryFactor: Byte = 8,
                          val keyLength: Byte = 16,
                          val saltLength: Byte = 20,
                          val salt: Array[Byte] = CryptoUtils.newStrongRandomByteArray(20)
                        )

class KeyInfo(val key: Array[Byte]) {
  def ivSize = 16
  var iv: Array[Byte] = _

  override def toString = s"KeyInfo(${new String(Base64.getEncoder.encode(iv))}, " +
    s"${new String(Base64.getEncoder.encode(key))}, $ivSize)"
}
