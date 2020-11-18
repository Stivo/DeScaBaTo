package ch.descabato.it.rocks

import ch.descabato.it.IntegrationTest

class PlainRocksBackupTest extends IntegrationTest {

    testWith("plain backup", " --compression gzip", "", 1, "10Mb")

    testWith("encrypted backup", " --passphrase testencryption --compression gzip", " --passphrase testencryption", 1, "10Mb")

}
