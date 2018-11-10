name := "core"

mainClass := Some("ch.descabato.CLI")

unmanagedSourceDirectories in Compile += new File("src/main/resources")

// Core dependencies
libraryDependencies ++= Seq(
    "com.typesafe.akka" %% "akka-actor" % "2.5.18",
    "com.typesafe.akka" %% "akka-stream" % "2.5.18",
    "org.rogach" %% "scallop" % "3.1.3",
    "org.ocpsoft.prettytime" % "prettytime" % "4.0.2.Final",
    "org.bouncycastle" % "bcprov-jdk15on" % "1.60"
)

// compressors
libraryDependencies ++= Seq(
  "org.iq80.snappy" % "snappy" % "0.4",
  "org.lz4" % "lz4-java" % "1.5.0",
  "org.tukaani" % "xz" % "1.8",
  "org.apache.commons" % "commons-compress" % "1.18"
)

// Logging
libraryDependencies ++= Seq(
	"ch.qos.logback" % "logback-classic" % "1.2.3",
	"com.typesafe.scala-logging" %% "scala-logging" % "3.9.0",
  "org.scala-lang" % "scala-reflect" % Common.scalaVersion
)

// Jackson / persistence
libraryDependencies ++= Seq(
	"com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.9.7"
)

// UI Dependencies
libraryDependencies ++= Seq(
  "com.nativelibs4java" % "bridj" % "0.7.0" exclude("com.google.android.tools", "dx"),
  "com.miglayout" % "miglayout-swing" % "5.2"
)

// remote dependencies
libraryDependencies ++= Seq(
  "org.apache.commons" % "commons-vfs2" % "2.2",
  "commons-net" % "commons-net" % "3.6",
  "com.amazonaws" % "aws-java-sdk-s3" % "1.11.447"
)


// Test dependencies
libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "3.0.5" % "test->*"
    excludeAll(
    ExclusionRule(organization = "org.seleniumhq.selenium"),
    ExclusionRule(organization = "org.eclipse.jetty"),
    ExclusionRule(organization = "org.testng"),
    ExclusionRule(organization = "org.jmock"),
    ExclusionRule(organization = "org.easymock"),
    ExclusionRule(organization = "org.mockito")
    ),
  "org.scalacheck" %% "scalacheck" % "1.14.0" % "test"
)

testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-o", "-u", "target/test-reports")

parallelExecution in Test := false

scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature")

enablePlugins(BuildInfoPlugin)

buildInfoPackage := "ch.descabato.version"

enablePlugins(PackPlugin)

packMain := Map("descabato" -> "ch.descabato.frontend.CLI")

packJvmOpts := Map("descabato" -> Seq("-Xms100m", "-Xmx2g"))

packJarNameConvention := "original"

packArchivePrefix := "descabato-core"
