package ch.descabato.core.util

import java.math.BigInteger
import java.security.SecureRandom
import java.util.zip.CRC32
import javax.crypto.Mac
import javax.crypto.spec.{IvParameterSpec, SecretKeySpec}

import akka.util.ByteString
import org.bouncycastle.crypto.generators.SCrypt

class KeyInfo(val key: ByteString) {
  def ivSize = 16

  var iv: ByteString = _
}

object CryptoUtils {

  def deriveIv(iv: ByteString, offset: Int): IvParameterSpec = {
    var bigInt = new BigInteger(iv.toArray)
    bigInt = bigInt.add(new BigInteger("" + offset / 16))
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

  def keySpec(keyInfo: KeyInfo) = new SecretKeySpec(keyInfo.key.toArray, "AES")

  def hmac(bytes: Array[Byte], keyInfo: KeyInfo): ByteString = {
    ByteString(newHmac(keyInfo).doFinal(bytes))
  }

  def newHmac(keyInfo: KeyInfo): Mac = {
    val sha256_HMAC = Mac.getInstance("HmacSHA256")
    val secret_key = new SecretKeySpec(keyInfo.key.toArray, "HmacSHA256")
    sha256_HMAC.init(secret_key)
    sha256_HMAC
  }

  implicit class PowerInt(val i: Int) extends AnyVal {
    def **(exp: Int): Int = Math.pow(i, exp).toInt
  }

  def keyDerive(passphrase: String, salt: Array[Byte], keyLength: Byte = 16, iterationsPower: Int = 12, memoryFactor: Int = 8): Array[Byte] = {
    SCrypt.generate(passphrase.getBytes("UTF-8"), salt, 2 ** iterationsPower, memoryFactor, 4, keyLength)
  }

}

object CrcUtil {

  def crc(bytes: ByteString): Int = {
    val crc = new CRC32()
    crc.update(bytes.toArray)
    crc.getValue().toInt
  }

  def checkCrc(bytes: ByteString, expected: Int, m: String = ""): Unit = {
    if (crc(bytes) != expected) {
      throw new IllegalArgumentException("Crc check failed: " + m)
    }
  }

}
