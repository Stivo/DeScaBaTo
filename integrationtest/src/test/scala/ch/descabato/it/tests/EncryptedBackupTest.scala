package ch.descabato.it.tests

import ch.descabato.it.IntegrationTest

class EncryptedBackupTest extends IntegrationTest {

    testWith("encrypted backup", " --threads 5 --compression none --volume-size 20Mb --passphrase mypassword", " --passphrase mypassword", 2, "20Mb")

}
