
// Additional Test dependencies
libraryDependencies ++= Seq(
   "commons-io" % "commons-io" % "2.8.0" % "test",
   "org.apache.commons" % "commons-exec" % "1.3" % "test"
)

parallelExecution in Test := false

testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-o", "-u", "target/test-reports")

