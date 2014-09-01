package ch.descabato.core.kvstore

import javax.crypto.spec.IvParameterSpec
import java.math.BigInteger
import java.util.Arrays
import javax.crypto.spec.SecretKeySpec
import java.security.{MessageDigest, SecureRandom}
import javax.crypto.Mac
import ch.descabato.utils.Utils
import org.bouncycastle.util.encoders.Base64
import scala.util.Random

//import org.bouncycastle.crypto.generators.SCrypt
import java.util.zip.CRC32

object CryptoUtils extends Utils {
  def deriveIv(iv: Array[Byte], offset: Int) = {
    var bigInt = new BigInteger(iv)
    bigInt = bigInt.add(new BigInteger(offset+""))
    var bytes = bigInt.toByteArray()
    if (bytes.length != iv.length) {
      l.warn(s"Have to update length for the iv ${bytes.length} vs ${iv.length}")
      while (bytes.length < iv.length) {
        bytes = Array(0.toByte) ++ bytes
      }
      while (bytes.length > iv.length) {
        bytes = bytes.drop(1)
      }
      l.warn("Iv in "+new String(Base64.encode(iv)))
      l.warn("offset "+offset)
      l.warn("Iv out "+new String(Base64.encode(bytes)))
    }
    new IvParameterSpec(bytes)
  }
  def newStrongRandomByteArray(ivLength: Short) = {
    val iv = Array.ofDim[Byte](ivLength)
    //SecureRandom.getInstance("PKCS11").nextBytes(iv)
    new Random().nextBytes(iv)
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
    val md = MessageDigest.getInstance("MD5")
    md.update(salt)
    md.update(passphrase.getBytes("UTF-8"))
    md.digest()
    //SCrypt.generate(passphrase.getBytes("UTF-8"), salt, 2**iterationsPower, memoryFactor, 4, keyLength)
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

