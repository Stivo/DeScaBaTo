package ch.descabato.it.rocks

import ch.descabato.it.IntegrationTestBase

trait RocksIntegrationTest extends IntegrationTestBase {
  override def mainClass: String = newMainClass

}
