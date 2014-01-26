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

// for truevfs
libraryDependencies += "javax.inject" % "javax.inject" % "1"

libraryDependencies += "com.google.code.findbugs" % "annotations" % "2.0.3"

// Test dependencies
libraryDependencies ++= Seq(
		"org.scalatest" %% "scalatest" % "2.0" % "test",
		"org.scalacheck" %% "scalacheck" % "1.10.0" % "test"
)

parallelExecution in Test := false

testOptions in Test += Tests.Argument("-oF")

scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature")

libraryDependencies += "org.rogach" %% "scallop" % "0.9.4"

packSettings

packMain := Map("descabato" -> "ch.descabato.CLI")

packPreserveOriginalJarName := true

buildInfoSettings

sourceGenerators in Compile <+= buildInfo

buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion)

buildInfoPackage := "ch.descabato.version"

EclipseKeys.createSrc := EclipseCreateSrc.Default + EclipseCreateSrc.Managed