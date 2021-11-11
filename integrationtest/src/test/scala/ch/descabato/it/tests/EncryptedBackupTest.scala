package ch.descabato.it.tests

import ch.descabato.it.IntegrationTest

class EncryptedBackupTest extends IntegrationTest {

    override val passphrase = Some("testencryption")

    testWith("encrypted backup", " --passphrase testencryption --compression gzip", " --passphrase testencryption", 1, "10Mb")

}
