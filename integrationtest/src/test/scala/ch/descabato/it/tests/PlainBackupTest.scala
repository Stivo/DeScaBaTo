package ch.descabato.it.tests

import ch.descabato.it.IntegrationTest

class PlainBackupTest extends IntegrationTest {

    testWith("plain backup", " --threads 1", "", 1, "10Mb")

}
