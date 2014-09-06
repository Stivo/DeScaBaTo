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
    "com.typesafe.akka" % "akka-actor_2.11" % "2.3.4",
    "org.rogach" %% "scallop" % "0.9.5",
    "org.ocpsoft.prettytime" % "prettytime" % "3.2.5.Final",
    "org.fusesource.jansi" % "jansi" % "1.11",
    "org.iq80.leveldb" % "leveldb" % "0.7",
    "org.fusesource.leveldbjni" % "leveldbjni-all" % "1.7",
    "org.bouncycastle" % "bcprov-jdk15on" % "1.51"
)

// compressors
libraryDependencies ++= Seq(
  "org.iq80.snappy" % "snappy" % "0.3",
  "net.jpountz.lz4" % "lz4" % "1.2.0",
  "org.tukaani" % "xz" % "1.5",
  "org.apache.commons" % "commons-compress" % "1.8.1"
)

// Logging
libraryDependencies ++= Seq(
	"ch.qos.logback" % "logback-classic" % "1.1.2",
	"com.typesafe.scala-logging" %% "scala-logging" % "3.1.0",
  "org.scala-lang" % "scala-reflect" % Common.scalaVersion
)

// Jackson / persistence
libraryDependencies ++= Seq(
	"com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.4.1",
	"com.fasterxml.jackson.dataformat" % "jackson-dataformat-smile" % "2.4.1",
    "de.undercouch" % "bson4jackson" % "2.4.0"
)

// Test dependencies
libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "2.2.1" % "test->*",
  "org.scalacheck" %% "scalacheck" % "1.11.5" % "test"
)

testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-o", "-u", "target/test-reports")

parallelExecution in Test := false

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
