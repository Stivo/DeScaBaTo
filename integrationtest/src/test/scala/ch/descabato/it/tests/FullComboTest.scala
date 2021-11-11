package ch.descabato.it.tests

import ch.descabato.it.IntegrationTest

class FullComboTest extends IntegrationTest {

  override val passphrase = Some("testpass")

  // TODO
  //  handle deleted files better (filemanager should not reuse those numbers)
  //  recreate log file handling from previous version
  //  check existing backups if files are corrupted because of this bug

  testWith("backup with crashes, encryption and smart compression",
    s" --compression smart --passphrase ${passphrase.get} --volume-size 30Mb", " --passphrase testpass", 3, "300mb", crash = true, redundancy = false)

}
