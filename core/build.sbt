import java.util.Date

name := "core"


Compile / unmanagedSourceDirectories += new File("src/main/resources")

// Core dependencies
libraryDependencies ++= Seq(
  "org.rogach" %% "scallop" % "4.1.0",
  "org.ocpsoft.prettytime" % "prettytime" % "5.0.2.Final",
  "org.bouncycastle" % "bcprov-jdk15on" % "1.69",
  "com.github.pathikrit" %% "better-files" % "3.9.1",
)

// compressors
libraryDependencies ++= Seq(
  "org.iq80.snappy" % "snappy" % "0.4",
  "org.lz4" % "lz4-java" % "1.8.0",
  "org.tukaani" % "xz" % "1.9",
  "org.apache.commons" % "commons-compress" % "1.21",
  "com.github.luben" % "zstd-jni" % "1.5.0-4"
)

// Logging
libraryDependencies ++= Seq(
  "ch.qos.logback" % "logback-classic" % "1.2.7",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.4",
  "org.scala-lang" % "scala-reflect" % Common.scalaVersion
)

// Jackson / persistence
libraryDependencies ++= Seq(
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.13.0"
)

// UI Dependencies
libraryDependencies ++= Seq(
  "com.nativelibs4java" % "bridj" % "0.7.0" exclude("com.google.android.tools", "dx"),
  // used in the progress reporting code that is currently unused
  "com.miglayout" % "miglayout-swing" % "5.3"
)

// remote dependencies
libraryDependencies ++= Seq(
  "com.amazonaws" % "aws-java-sdk-s3" % "1.12.109"
)

val scalaTestVersion = "3.2.10"

// Test dependencies
libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % scalaTestVersion % "test"
    excludeAll(
    ExclusionRule(organization = "org.seleniumhq.selenium"),
    ExclusionRule(organization = "org.eclipse.jetty"),
    ExclusionRule(organization = "org.testng"),
    ExclusionRule(organization = "org.jmock"),
    ExclusionRule(organization = "org.easymock"),
    ExclusionRule(organization = "org.mockito")
  ),
  "org.scalatestplus" %% "scalacheck-1-14" % "3.2.2.0" % Test,
  "org.scalatest" %% "scalatest-matchers-core" % scalaTestVersion % Test,
  "org.scalactic" %% "scalactic" % scalaTestVersion % "test"
    excludeAll(
    ExclusionRule(organization = "org.seleniumhq.selenium"),
    ExclusionRule(organization = "org.eclipse.jetty"),
    ExclusionRule(organization = "org.testng"),
    ExclusionRule(organization = "org.jmock"),
    ExclusionRule(organization = "org.easymock"),
    ExclusionRule(organization = "org.mockito")
  )
)

libraryDependencies += "com.thesamet.scalapb" %% "scalapb-runtime" % scalapb.compiler.Version.scalapbVersion % "protobuf"

Compile / PB.targets := Seq(
  scalapb.gen(grpc = false, lenses = false) -> (Compile / sourceManaged).value / "scalapb"
)

Test / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-o", "-u", "target/test-reports")

Test / parallelExecution := false

scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature")

enablePlugins(PackPlugin)

packMain := Map("descabato" -> "ch.descabato.Main")

packJvmOpts := Map("descabato" -> Seq("-Xmx2g", "-Xms1g", "-XX:NewSize=1g", "-XX:MaxNewSize=1g", "-XX:MaxPermSize=1g"))

packJarNameConvention := "original"

packArchivePrefix := "descabato-core"

lazy val generateBuildInfo = taskKey[Seq[File]]("Generates the build information")

generateBuildInfo := {
  val file = (Compile / resourceManaged).value / "buildinfo.properties"
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

Compile / resourceGenerators += generateBuildInfo