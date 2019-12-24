package ch.descabato.rocks.ftp

import ch.descabato.rocks.Main

object RunLocal {

  def main(args: Array[String]): Unit = {
    val destination = "l:/backup"
    //    FileUtils.deleteAll(new File(destination))
    //    Main.main(Array("backup", destination, "l:/workspace"))
    Main.main(Array("ftp", destination))
  }

}
