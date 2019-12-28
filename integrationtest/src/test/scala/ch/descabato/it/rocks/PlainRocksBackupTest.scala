package ch.descabato.it.rocks

import ch.descabato.it.IntegrationTest
import ch.descabato.it.RocksIntegrationTest

class PlainRocksBackupTest extends IntegrationTest with RocksIntegrationTest {

    override val verifyEnabled: Boolean = false

    testWith("plain backup", " --compression gzip --threads 1", "", 1, "10Mb")

    testWith("encrypted backup", " --passphrase testencryption --compression gzip --threads 1", " --passphrase testencryption", 1, "10Mb")

}
