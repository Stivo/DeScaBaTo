import sbt._

object MyBuild extends Build {

  lazy val root = Project("root", file(".")) dependsOn(sumac % "compile")
  lazy val sumac = ProjectRef(uri("https://github.com/Stivo/Sumac.git#shortcuts"), "core")

  val mainClass = Some("backup.CommandLine")
  
}
