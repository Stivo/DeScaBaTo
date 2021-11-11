package ch.descabato.it.rocks

import ch.descabato.it.IntegrationTest

class PlainBackupTest extends IntegrationTest {

    testWith("plain backup", " --compression gzip", "", 1, "10Mb")

}
