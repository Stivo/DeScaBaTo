import de.johoop.jacoco4sbt._
import JacocoPlugin._

name := "browser"

organization := "ch.descabato"

version := Common.version

scalaVersion := Common.scalaVersion

mainClass := Some("ch.descabato.browser.Main")

packageArchetype.java_application

unmanagedSourceDirectories in Compile <++= baseDirectory { base =>
  Seq(
    base / "src/main/resources"
  )
}

libraryDependencies ++= Seq(
		"org.apache.commons" % "commons-vfs2" % "2.0"
//		"commons-httpclient" % "commons-httpclient" % "3.1",
//		"commons-net" % "commons-net" % "3.3"
)

// Test dependencies
libraryDependencies ++= Seq(
		"org.apache.ftpserver" % "ftpserver-core" % "1.0.6" % "test",
		"org.scalatest" %% "scalatest" % "2.0" % "test",
		"org.scalacheck" %% "scalacheck" % "1.10.0" % "test"
)

scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature", "-Xlint")

// Dependencies for the otros vfs browser
libraryDependencies ++= Seq(
	"commons-configuration" % "commons-configuration" % "1.8",
	"commons-io" % "commons-io" % "2.1",
//	"com.google.guava" % "guava" % "15.0",
	"com.miglayout" % "miglayout-swing" % "4.2",
	"org.swinglabs.swingx" % "swingx-all" % "1.6.5-1",
	"net.java.dev.jgoodies" % "looks" % "2.1.4",
	"com.intellij" % "annotations" % "9.0.4",
	"org.ocpsoft.prettytime" % "prettytime" % "3.1.0.Final",
	"com.github.insubstantial" % "substance" % "7.2.1"
)

packSettings

packMain := Map("descabato" -> "ch.descabato.browser.Main")

packJvmOpts := Map("descabato" -> Seq("-Xms100m", "-Xmx500m", "-XX:NewRatio=1","-XX:+UseParNewGC"))

packPreserveOriginalJarName := true

packResourceDir := "../releases/0.3.0"