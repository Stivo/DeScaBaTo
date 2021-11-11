import java.util.Date

name := "core"


Compile / unmanagedSourceDirectories += new File("src/main/resources")

// Core dependencies
libraryDependencies ++= Seq(
  "org.rogach" %% "scallop" % "4.0.2",
  "org.ocpsoft.prettytime" % "prettytime" % "5.0.0.Final",
  "org.bouncycastle" % "bcprov-jdk15on" % "1.68",
  "com.github.pathikrit" %% "better-files" % "3.9.1",
)

// compressors
libraryDependencies ++= Seq(
  "org.iq80.snappy" % "snappy" % "0.4",
  "org.lz4" % "lz4-java" % "1.7.1",
  "org.tukaani" % "xz" % "1.9",
  "org.apache.commons" % "commons-compress" % "1.20",
  "com.github.luben" % "zstd-jni" % "1.4.9-5"
)

// Logging
libraryDependencies ++= Seq(
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.3",
  "org.scala-lang" % "scala-reflect" % Common.scalaVersion
)

// Jackson / persistence
libraryDependencies ++= Seq(
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.12.3"
)

// UI Dependencies
libraryDependencies ++= Seq(
  "com.nativelibs4java" % "bridj" % "0.7.0" exclude("com.google.android.tools", "dx"),
  "com.miglayout" % "miglayout-swing" % "5.3"
)

// remote dependencies
libraryDependencies ++= Seq(
  "com.amazonaws" % "aws-java-sdk-s3" % "1.11.1004"
)


// Test dependencies
libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "3.2.8" % "test"
    excludeAll(
    ExclusionRule(organization = "org.seleniumhq.selenium"),
    ExclusionRule(organization = "org.eclipse.jetty"),
    ExclusionRule(organization = "org.testng"),
    ExclusionRule(organization = "org.jmock"),
    ExclusionRule(organization = "org.easymock"),
    ExclusionRule(organization = "org.mockito")
  ),
  "org.scalatestplus" %% "scalacheck-1-14" % "3.2.2.0" % Test,
  "org.scalatest" %% "scalatest-matchers-core" % "3.2.8" % Test,
  "org.scalactic" %% "scalactic" % "3.2.8" % "test"
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