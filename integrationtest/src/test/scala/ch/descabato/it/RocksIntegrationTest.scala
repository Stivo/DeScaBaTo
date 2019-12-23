package ch.descabato.it

abstract class RocksIntegrationTest extends IntegrationTest {
  override def mainClass: String = "ch.descabato.rocks.Main"

  override val verifyEnabled: Boolean = false
}

