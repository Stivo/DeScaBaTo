package ch.descabato.it.tests

import javax.crypto.Cipher

import ch.descabato.it.IntegrationTest

class EncryptedBackupTest extends IntegrationTest {

  testWith("encrypted backup", " --threads 5 --compression none --volume-size 20Mb --passphrase mypassword", " --passphrase mypassword", 2, "20Mb")

  private val maxKeyLengthAllowed: Int = Cipher.getMaxAllowedKeyLength("AES")
  println(s"Keylength allowed here: $maxKeyLengthAllowed")

  if (maxKeyLengthAllowed >= 256) {
    testWith("encrypted backup with aes256", " --threads 5 --compression none --volume-size 20Mb --passphrase mypassword --keylength 256", " --passphrase mypassword", 1, "20Mb")
  }

}
