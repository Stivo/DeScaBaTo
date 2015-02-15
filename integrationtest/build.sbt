
version := Common.version

scalaVersion := Common.scalaVersion

// Additional Test dependencies
libraryDependencies ++= Seq(
   "commons-io" % "commons-io" % "2.4" % "test",
   "org.apache.commons" % "commons-exec" % "1.3" % "test"
)

parallelExecution in Test := false

testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-o", "-u", "target/test-reports")

test in Test <<= (test in Test).dependsOn(pack in core)
