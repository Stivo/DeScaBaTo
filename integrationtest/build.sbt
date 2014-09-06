import de.johoop.jacoco4sbt._
import JacocoPlugin._

version := Common.version

scalaVersion := Common.scalaVersion

// Test dependencies
libraryDependencies ++= Seq(
	"org.scalatest" %% "scalatest" % "2.2.1" % "test->*",
	"org.scalacheck" %% "scalacheck" % "1.11.5" % "test"
)

parallelExecution in Test := false

parallelExecution in jacoco.Config := false

jacoco.settings

testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-o", "-u", "target/test-reports")

test in Test <<= (test in Test).dependsOn(pack in core)

libraryDependencies += "org.apache.commons" % "commons-exec" % "1.2" % "test"