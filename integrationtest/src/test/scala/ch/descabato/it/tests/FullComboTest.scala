package ch.descabato.it.tests

import ch.descabato.it.IntegrationTest
import org.scalatest.Ignore

@Ignore
class FullComboTest extends IntegrationTest {

  testWith("backup with crashes, encryption, multiple threads and smart compression",
    " --threads 10 --compression smart --passphrase testpass --volume-size 50Mb", " --passphrase testpass", 3, "300mb", crash = true, redundancy = false)

}
