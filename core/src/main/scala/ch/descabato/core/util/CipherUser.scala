package ch.descabato.core.util

import java.security.Security
import javax.crypto.Cipher

import ch.descabato.core_old.kvstore.{CryptoUtils, EncryptionInfo, KeyInfo}
import org.bouncycastle.jce.provider.BouncyCastleProvider

class CipherUser(key: Array[Byte]) {

  Security.addProvider(new BouncyCastleProvider())
  val cipher = Cipher.getInstance("AES/CTR/NoPadding", "BC")

  protected var keyInfo: KeyInfo = null
  protected var encryptionBoundary: Long = -1

  protected var encryptionInfo = new EncryptionInfo()
  protected val magicMarker: Array[Byte] = "KVStore".getBytes("UTF-8")
  protected val kvStoreVersion: Array[Byte] = Array(0.toByte)

  protected def initializeCipher(mode: Int, keyInfo: KeyInfo, offset: Int = 0) = {
    val iv = CryptoUtils.deriveIv(keyInfo.iv, offset)
    cipher.init(mode, CryptoUtils.keySpec(keyInfo), iv)
  }

}
