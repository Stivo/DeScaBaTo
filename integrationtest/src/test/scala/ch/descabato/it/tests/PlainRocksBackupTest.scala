package ch.descabato.it.tests

import ch.descabato.it.RocksIntegrationTest

class PlainRocksBackupTest extends RocksIntegrationTest {

    testWith("plain backup", " --compression gzip --threads 1", "", 1, "10Mb")

    testWith("encrypted backup", " --passphrase testencryption --compression gzip --threads 1", " --passphrase testencryption", 1, "10Mb")

}
