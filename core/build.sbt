import de.johoop.jacoco4sbt._
import JacocoPlugin._

name := "core"

organization := "ch.descabato"

version := "0.1.0-SNAPSHOT"

scalaVersion := "2.10.3"

mainClass := Some("ch.descabato.CLI")

packageArchetype.java_application

unmanagedSourceDirectories in Compile <++= baseDirectory { base =>
  Seq(
    base / "src/main/resources",
    base / "src/test/resources"
  )
}

libraryDependencies += "org.fusesource.jansi" % "jansi" % "1.11"

libraryDependencies += "org.ocpsoft.prettytime" % "prettytime" % "3.1.0.Final"

libraryDependencies ++= Seq(
		"ch.qos.logback" % "logback-classic" % "1.0.13",
		"com.typesafe" % "scalalogging-slf4j_2.10" % "1.0.1"
)

libraryDependencies ++= Seq(
		"com.fasterxml.jackson.module" % "jackson-module-scala_2.10" % "2.3.1",
		"com.fasterxml.jackson.dataformat" % "jackson-dataformat-smile" % "2.3.1"
)

// Test dependencies
libraryDependencies ++= Seq(
		"org.apache.ftpserver" % "ftpserver-core" % "1.0.6" % "test",
		"org.scalatest" %% "scalatest" % "2.0" % "test",
		"org.scalacheck" %% "scalacheck" % "1.10.0" % "test"
)

parallelExecution in Test := false

jacoco.settings

org.scalastyle.sbt.ScalastylePlugin.Settings

parallelExecution in jacoco.Config := false

//libraryDependencies += "com.typesafe.akka" %% "akka-actor" % "2.2.3"

libraryDependencies += "org.tukaani" % "xz" % "1.4"

testOptions in Test += Tests.Argument("-oF")

scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature", "-Xlint")

libraryDependencies += "org.rogach" %% "scallop" % "0.9.4"
