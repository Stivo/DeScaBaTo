package ch.descabato.it.tests

import ch.descabato.it.IntegrationTest
import ch.descabato.it.rocks.RocksIntegrationTest

class FullComboTest extends IntegrationTest with RocksIntegrationTest {

  testWith("backup with crashes, encryption, multiple threads and smart compression",
    " --threads 10 --compression smart --passphrase testpass --volume-size 50Mb", " --passphrase testpass", 3, "300mb", crash = true, redundancy = false)

}
