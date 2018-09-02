package ch.descabato.it.tests

import ch.descabato.it.IntegrationTest
import org.scalatest.Ignore

class BackupWithCrashesTest extends IntegrationTest {

    testWith("backup with crashes", " --no-gui --compression deflate --volume-size 10Mb", "", 2, "100Mb", crash = true, redundancy = false)

}
