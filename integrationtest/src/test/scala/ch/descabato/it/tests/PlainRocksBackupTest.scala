package ch.descabato.it.tests

import ch.descabato.it.RocksIntegrationTest

class PlainRocksBackupTest extends RocksIntegrationTest {

    testWith("plain backup", " --compression gzip --threads 1", "", 1, "10Mb")

}
