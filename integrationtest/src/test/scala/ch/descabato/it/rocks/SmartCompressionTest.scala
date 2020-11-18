package ch.descabato.it.rocks

import ch.descabato.it.IntegrationTest

class SmartCompressionTest extends IntegrationTest {

    testWith("backup with smart compression", " --compression smart --volume-size 20Mb", "", 1, "200Mb", crash = false)

}
