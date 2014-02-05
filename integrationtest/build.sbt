import de.johoop.jacoco4sbt._
import JacocoPlugin._

// Test dependencies
libraryDependencies ++= Seq(
    "org.apache.ftpserver" % "ftpserver-core" % "1.0.6" % "test",
    "org.scalatest" %% "scalatest" % "2.0" % "test",
    "org.scalacheck" %% "scalacheck" % "1.10.0" % "test"
)

parallelExecution in Test := false

parallelExecution in jacoco.Config := false

jacoco.settings

testOptions in Test += Tests.Argument("-oF")
