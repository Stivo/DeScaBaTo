package ch.descabato.web

import ch.descabato.frontend.CLI

/**
  * Created by Stivo on 26.12.2016.
  */
object Main {
  def main(args: Array[String]) {
    CLI.main(Array("web", "e:/backups/pics"))
    //CLI.main(args)
  }
}
