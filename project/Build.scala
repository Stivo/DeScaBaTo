import sbt._
import Keys._
//import com.typesafe.sbtscalariform.ScalariformPlugin
//import scalariform.formatter.preferences._

object HelloBuild extends Build {
  
  val core = Project(id = "core", base = file("core"))
  
  val browser = Project(id = "browser", base = file("browser")) dependsOn(core)
  
}
