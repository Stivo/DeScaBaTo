package ch.descabato.ui

import ch.descabato.frontend.CLI

object Main {
  def main(args: Array[String]) {
    CLI.main(Array("browse", "e:/backups/pics"))
//    CLI.main(Array("browse", "l:/backup"))
    //CLI.main(args)
  }
}
