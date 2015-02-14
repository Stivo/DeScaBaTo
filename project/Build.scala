import sbt.Keys._
import sbt._
//import com.typesafe.sbtscalariform.ScalariformPlugin
//import scalariform.formatter.preferences._

object DeScaBaToBuild extends Build {
  scalaVersion in ThisBuild := Common.scalaVersion
  
  val core = Project(id = "core", base = file("core"), 
      settings = Project.defaultSettings ++ commonSettings)
  
  val browser = Project(id = "browser", base = file("browser"),
      settings = Project.defaultSettings ++ commonSettings) dependsOn(core)
  
  val it = Project(id = "it", base = file("integrationtest"),
      settings = Project.defaultSettings ++ commonSettings) dependsOn(core % "test->test")
  
  val commonSettings = {
    net.virtualvoid.sbt.graph.Plugin.graphSettings ++ org.scalastyle.sbt.ScalastylePlugin.Settings
  }
  
}
