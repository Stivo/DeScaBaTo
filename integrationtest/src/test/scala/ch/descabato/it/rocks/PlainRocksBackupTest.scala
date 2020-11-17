package ch.descabato.it.rocks

import ch.descabato.it.IntegrationTest

class PlainRocksBackupTest extends IntegrationTest with RocksIntegrationTest {

    testWith("plain backup", " --compression gzip --threads 1", "", 1, "10Mb")

    testWith("encrypted backup", " --passphrase testencryption --compression gzip --threads 1", " --passphrase testencryption", 1, "10Mb")

}
