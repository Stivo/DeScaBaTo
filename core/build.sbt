import java.util.Date

name := "core"

mainClass := Some("ch.descabato.CLI")

unmanagedSourceDirectories in Compile += new File("src/main/resources")

// Core dependencies
libraryDependencies ++= Seq(
  "org.rogach" %% "scallop" % "3.3.1",
  "org.ocpsoft.prettytime" % "prettytime" % "4.0.6.Final",
  "org.bouncycastle" % "bcprov-jdk15on" % "1.66",
  "org.rocksdb" % "rocksdbjni" % "6.4.6",
  "com.github.pathikrit" %% "better-files" % "3.9.1",
)

// compressors
libraryDependencies ++= Seq(
  "org.iq80.snappy" % "snappy" % "0.4",
  "org.lz4" % "lz4-java" % "1.7.1",
  "org.tukaani" % "xz" % "1.8",
  "org.apache.commons" % "commons-compress" % "1.20",
  "com.github.luben" % "zstd-jni" % "1.4.5-6"
)

// Logging
libraryDependencies ++= Seq(
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2",
  "org.scala-lang" % "scala-reflect" % Common.scalaVersion
)

// Jackson / persistence
libraryDependencies ++= Seq(
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.9.10"
)

// UI Dependencies
libraryDependencies ++= Seq(
  "com.nativelibs4java" % "bridj" % "0.7.0" exclude("com.google.android.tools", "dx"),
  "com.miglayout" % "miglayout-swing" % "5.2"
)

// remote dependencies
libraryDependencies ++= Seq(
  "org.apache.commons" % "commons-vfs2" % "2.6.0",
  "commons-net" % "commons-net" % "3.6",
  "com.amazonaws" % "aws-java-sdk-s3" % "1.11.880"
)


// Test dependencies
libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "3.2.2" % "test"
    excludeAll(
    ExclusionRule(organization = "org.seleniumhq.selenium"),
    ExclusionRule(organization = "org.eclipse.jetty"),
    ExclusionRule(organization = "org.testng"),
    ExclusionRule(organization = "org.jmock"),
    ExclusionRule(organization = "org.easymock"),
    ExclusionRule(organization = "org.mockito")
  ),
  "org.scalatestplus" %% "scalatestplus-scalacheck" % "1.0.0-M2" % Test,
  "org.scalatest" %% "scalatest-matchers-core" % "3.2.2" % Test,
  "org.scalactic" %% "scalactic" % "3.2.2" % "test"
    excludeAll(
    ExclusionRule(organization = "org.seleniumhq.selenium"),
    ExclusionRule(organization = "org.eclipse.jetty"),
    ExclusionRule(organization = "org.testng"),
    ExclusionRule(organization = "org.jmock"),
    ExclusionRule(organization = "org.easymock"),
    ExclusionRule(organization = "org.mockito")
  ),
  "org.scalacheck" %% "scalacheck" % "1.14.3" % "test"
)

PB.targets in Compile := Seq(
  scalapb.gen() -> (sourceManaged in Compile).value
)

testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-o", "-u", "target/test-reports")

parallelExecution in Test := false

scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature")

enablePlugins(PackPlugin)

packMain := Map("descabato" -> "ch.descabato.rocks.Main")

packJvmOpts := Map("descabato" -> Seq("-Xms100m", "-Xmx2g"))

packJarNameConvention := "original"

packArchivePrefix := "descabato-core"

lazy val generateBuildInfo = taskKey[Seq[File]]("Generates the build information")

generateBuildInfo := {
  val file = (resourceManaged in Compile).value / "buildinfo.properties"
  //  val pb = new ProcessBuilder("git", "branch", "--show-current")
  //  val process = pb.start()
  //  val outputStream = process.getInputStream
  //  val branch = new BufferedReader(new InputStreamReader(outputStream)).readLine()
  IO.write(file,
    s"""
       |version = ${version.value}
       |scalaVersion = ${scalaVersion.value}
       |sbtVersion = ${sbtVersion.value}
       |builtAt = ${new Date()}
       |builtAtMillis = ${System.currentTimeMillis()}
       |""".stripMargin)
  Seq(file)
}

resourceGenerators in Compile += generateBuildInfo