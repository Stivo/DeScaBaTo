name := "core"

mainClass := Some("ch.descabato.CLI")

unmanagedSourceDirectories in Compile += new File("src/main/resources")

// Core dependencies
libraryDependencies ++= Seq(
    "com.typesafe.akka" %% "akka-actor" % "2.5.8",
    "com.typesafe.akka" %% "akka-stream" % "2.5.8",
    "org.rogach" %% "scallop" % "3.1.1",
    "org.ocpsoft.prettytime" % "prettytime" % "4.0.1.Final",
    "org.fusesource.jansi" % "jansi" % "1.11",
    "org.bouncycastle" % "bcprov-jdk15on" % "1.59"
)

// compressors
libraryDependencies ++= Seq(
  "org.iq80.snappy" % "snappy" % "0.4",
  "org.lz4" % "lz4-java" % "1.4.1",
  "org.tukaani" % "xz" % "1.8",
  "org.apache.commons" % "commons-compress" % "1.15"
)

// Logging
libraryDependencies ++= Seq(
	"ch.qos.logback" % "logback-classic" % "1.2.3",
	"com.typesafe.scala-logging" %% "scala-logging" % "3.7.2",
  "org.scala-lang" % "scala-reflect" % Common.scalaVersion
)

// Jackson / persistence
libraryDependencies ++= Seq(
	"com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.9.2",
	"com.fasterxml.jackson.dataformat" % "jackson-dataformat-smile" % "2.9.2"
)

// UI Dependencies
libraryDependencies ++= Seq(
  "com.nativelibs4java" % "bridj" % "0.7.0" exclude("com.google.android.tools", "dx"),
  "com.miglayout" % "miglayout-swing" % "5.0"
)

// remote dependencies
libraryDependencies ++= Seq(
  "org.apache.commons" % "commons-vfs2" % "2.2",
  "commons-net" % "commons-net" % "3.6",
  "com.google.guava" % "guava" % "16.0.1",
  "com.amazonaws" % "aws-java-sdk-s3" % "1.11.257"
)


// Test dependencies
libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "3.0.4" % "test->*"
    excludeAll(
    ExclusionRule(organization = "org.seleniumhq.selenium"),
    ExclusionRule(organization = "org.eclipse.jetty"),
    ExclusionRule(organization = "org.testng"),
    ExclusionRule(organization = "org.jmock"),
    ExclusionRule(organization = "org.easymock"),
    ExclusionRule(organization = "org.mockito")
    ),
  "org.scalacheck" %% "scalacheck" % "1.13.5" % "test"
)

testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-o", "-u", "target/test-reports")

parallelExecution in Test := false

scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature")

enablePlugins(BuildInfoPlugin)

buildInfoPackage := "ch.descabato.version"

enablePlugins(PackPlugin)

packMain := Map("descabato" -> "ch.descabato.frontend.CLI")

packJvmOpts := Map("descabato" -> Seq("-Xms100m", "-Xmx1g"))

packJarNameConvention := "original"

packArchivePrefix := "descabato-core"
