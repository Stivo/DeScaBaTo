package ch.descabato.utils

import java.util.Properties

object BuildInfo {

  private val props = {
    val props = new Properties()
    val stream = getClass.getResourceAsStream("/buildinfo.properties")
    props.load(stream)
    stream.close()
    props
  }

  val version: String = props.getProperty("version")
  val scalaVersion: String = props.getProperty("scalaVersion")

  def main(args: Array[String]): Unit = {
    println(version)
    println(scalaVersion)
  }

}
