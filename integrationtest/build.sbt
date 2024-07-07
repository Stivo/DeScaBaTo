
// Additional Test dependencies
libraryDependencies ++= Seq(
   "commons-io" % "commons-io" % "2.16.1" % "test",
   "org.apache.commons" % "commons-exec" % "1.4.0" % "test"
)

Test / parallelExecution := false

Test / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-o", "-u", "target/test-reports")

