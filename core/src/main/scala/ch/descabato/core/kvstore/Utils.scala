package ch.descabato.core.kvstore

import javax.crypto.spec.IvParameterSpec
import java.math.BigInteger
import java.util.Arrays
import javax.crypto.spec.SecretKeySpec
import java.security.SecureRandom
import javax.crypto.Mac
import org.bouncycastle.crypto.generators.SCrypt
import java.util.zip.CRC32

object CryptoUtils {
  def deriveIv(iv: Array[Byte], offset: Int) = {
    var bigInt = new BigInteger(iv)
    bigInt = bigInt.add(new BigInteger(offset+""))
    new IvParameterSpec(bigInt.toByteArray())
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

