package ch.descabato.core.util

import org.bouncycastle.jce.provider.BouncyCastleProvider

import java.security.Security
import javax.crypto.Cipher

class CipherUser {

  Security.addProvider(new BouncyCastleProvider())
  val cipher: Cipher = Cipher.getInstance("AES/CTR/NoPadding", "BC")

  protected var keyInfo: KeyInfo = null
  protected var encryptionBoundary: Long = -1

  protected var encryptionInfo = new EncryptionInfo()
  protected val magicMarker: Array[Byte] = "KVStore".getBytes("UTF-8")
  protected val kvStoreVersion: Array[Byte] = Array(0.toByte)

  protected def initializeCipher(mode: Int, keyInfo: KeyInfo, offset: Int = 0): Unit = {
    val iv = CryptoUtils.deriveIv(keyInfo.iv, offset)
    cipher.init(mode, CryptoUtils.keySpec(keyInfo), iv)
  }

}
