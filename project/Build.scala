import sbt.Keys._
import sbt._
//import com.typesafe.sbtscalariform.ScalariformPlugin
//import scalariform.formatter.preferences._

object DeScaBaToBuild extends Build {
  scalaVersion in ThisBuild := Common.scalaVersion
  
  val core = Project(id = "core", base = file("core"), 
      settings = Defaults.defaultSettings ++ commonSettings)

  val ui = Project(id = "ui", base = file("ui"),
    settings = Defaults.defaultSettings ++ commonSettings) dependsOn(core)

  val it = Project(id = "it", base = file("integrationtest"),
      settings = Defaults.defaultSettings ++ commonSettings) dependsOn(core % "test->test")


  val commonSettings = {
    List(
       artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
         s"${module.organization}.${artifact.name}-${module.revision}.${artifact.extension}"
       },
       organization := Common.organization,
       version := Common.version,
       scalaVersion := Common.scalaVersion
    )
  }
  
}
