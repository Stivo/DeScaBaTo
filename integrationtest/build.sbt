import de.johoop.jacoco4sbt.JacocoPlugin._
import de.johoop.jacoco4sbt._

version := Common.version

scalaVersion := Common.scalaVersion

// Test dependencies
libraryDependencies ++= Seq(
	"org.scalatest" %% "scalatest" % "2.2.4" % "test->*",
	"org.scalacheck" %% "scalacheck" % "1.12.2" % "test"
)

parallelExecution in Test := false

parallelExecution in jacoco.Config := false

jacoco.settings

testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-o", "-u", "target/test-reports")

test in Test <<= (test in Test).dependsOn(pack in core)

libraryDependencies += "org.apache.commons" % "commons-exec" % "1.3" % "test"