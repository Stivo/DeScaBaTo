name := "core"

mainClass := Some("ch.descabato.CLI")

unmanagedSourceDirectories in Compile += new File("src/main/resources")

// Core dependencies
libraryDependencies ++= Seq(
    "com.typesafe.akka" %% "akka-actor" % "2.4.14",
    "org.rogach" %% "scallop" % "2.0.5",
    "org.ocpsoft.prettytime" % "prettytime" % "4.0.1.Final",
    "org.fusesource.jansi" % "jansi" % "1.11",
    "org.bouncycastle" % "bcprov-jdk15on" % "1.55"
)

// compressors
libraryDependencies ++= Seq(
  "org.iq80.snappy" % "snappy" % "0.4",
  "net.jpountz.lz4" % "lz4" % "1.3.0",
  "org.tukaani" % "xz" % "1.6",
  "org.apache.commons" % "commons-compress" % "1.12"
)

// Logging
libraryDependencies ++= Seq(
	"ch.qos.logback" % "logback-classic" % "1.1.8",
	"com.typesafe.scala-logging" %% "scala-logging" % "3.5.0",
  "org.scala-lang" % "scala-reflect" % Common.scalaVersion
)

// Jackson / persistence
libraryDependencies ++= Seq(
	"com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.8.4",
	"com.fasterxml.jackson.dataformat" % "jackson-dataformat-smile" % "2.8.4"
)

// UI Dependencies
libraryDependencies ++= Seq(
  "com.nativelibs4java" % "bridj" % "0.7.0" exclude("com.google.android.tools", "dx"),
  "com.miglayout" % "miglayout-swing" % "5.0"
)

// Test dependencies
libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "3.0.1" % "test->*"
    excludeAll(
    ExclusionRule(organization = "org.seleniumhq.selenium"),
    ExclusionRule(organization = "org.eclipse.jetty"),
    ExclusionRule(organization = "org.testng"),
    ExclusionRule(organization = "org.jmock"),
    ExclusionRule(organization = "org.easymock"),
    ExclusionRule(organization = "org.mockito")
    ),
  "org.scalacheck" %% "scalacheck" % "1.13.4" % "test"
)

testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-o", "-u", "target/test-reports")

parallelExecution in Test := false

scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature")

buildInfoSettings

sourceGenerators in Compile <+= buildInfo

buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion)

buildInfoPackage := "ch.descabato.version"

enablePlugins(PackPlugin)

packMain := Map("descabato" -> "ch.descabato.frontend.CLI")

packJvmOpts := Map("descabato" -> Seq("-Xms100m", "-Xmx1g"))

packJarNameConvention := "original"

packArchivePrefix := "descabato-core"
