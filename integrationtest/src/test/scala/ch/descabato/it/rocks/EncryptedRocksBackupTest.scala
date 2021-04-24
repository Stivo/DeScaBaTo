package ch.descabato.it.rocks

import ch.descabato.it.IntegrationTest

class EncryptedRocksBackupTest extends IntegrationTest {

    override val passphrase = Some("testencryption")

    testWith("encrypted backup", " --passphrase testencryption --compression gzip", " --passphrase testencryption", 1, "10Mb")

}
