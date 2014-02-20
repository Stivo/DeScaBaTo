name := "core"

organization := "ch.descabato"

version := Common.version

scalaVersion := Common.scalaVersion

mainClass := Some("ch.descabato.CLI")

packageArchetype.java_application

normalizedName in Universal := "descabato"

unmanagedSourceDirectories in Compile <++= baseDirectory { base =>
  Seq(
    base / "src/main/resources"
  )
}

// Core dependencies
libraryDependencies ++= Seq(
    "com.typesafe.akka" % "akka-actor_2.10" % "2.2.3",
    "org.rogach" %% "scallop" % "0.9.4",
    "org.iq80.snappy" % "snappy" % "0.3",
    "org.ocpsoft.prettytime" % "prettytime" % "3.1.0.Final",
    "org.fusesource.jansi" % "jansi" % "1.11"
)

// compressors
libraryDependencies ++= Seq(
  "org.iq80.snappy" % "snappy" % "0.3",
  "net.jpountz.lz4" % "lz4" % "1.2.0"
)

// Logging
libraryDependencies ++= Seq(
		"ch.qos.logback" % "logback-classic" % "1.0.13",
		"com.typesafe" % "scalalogging-slf4j_2.10" % "1.1.0",
    "org.scala-lang" % "scala-reflect" % Common.scalaVersion
)

// Jackson / persistence
libraryDependencies ++= Seq(
		"com.fasterxml.jackson.module" % "jackson-module-scala_2.10" % "2.3.1",
		"com.fasterxml.jackson.dataformat" % "jackson-dataformat-smile" % "2.3.1",
    "de.undercouch" % "bson4jackson" % "2.3.1"
)

// truevfs
libraryDependencies ++= Seq(
    "javax.inject" % "javax.inject" % "1",
    "com.google.code.findbugs" % "annotations" % "2.0.3",
	"org.apache.commons" % "commons-compress" % "1.7"
)

// Test dependencies
libraryDependencies ++= Seq(
		"org.scalatest" %% "scalatest" % "2.0" % "test",
		"org.scalacheck" %% "scalacheck" % "1.10.0" % "test"
)

parallelExecution in Test := false

testOptions in Test += Tests.Argument("-oF")

scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature")

libraryDependencies += "com.nativelibs4java" % "bridj" % "0.6.2" exclude("com.google.android.tools", "dx")

libraryDependencies += "com.miglayout" % "miglayout-swing" % "4.2"

packSettings

packMain := Map("descabato" -> "ch.descabato.frontend.CLI")

packJvmOpts := Map("descabato" -> Seq("-Xms100m", "-Xmx500m", "-XX:NewRatio=1","-XX:+UseParNewGC"))

packPreserveOriginalJarName := true

buildInfoSettings

sourceGenerators in Compile <+= buildInfo

buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion)

buildInfoPackage := "ch.descabato.version"

EclipseKeys.createSrc := EclipseCreateSrc.Default + EclipseCreateSrc.Managed
