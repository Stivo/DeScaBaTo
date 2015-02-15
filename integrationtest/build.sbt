
version := Common.version

scalaVersion := Common.scalaVersion

// Test dependencies
libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "2.2.4" % "test->*"
    excludeAll(
    ExclusionRule(organization = "org.seleniumhq.selenium"),
    ExclusionRule(organization = "org.eclipse.jetty"),
    ExclusionRule(organization = "org.testng"),
    ExclusionRule(organization = "org.jmock"),
    ExclusionRule(organization = "org.easymock"),
    ExclusionRule(organization = "org.mockito"),
    ExclusionRule(organization = "org.apache.ant")
    ),
	"org.scalacheck" %% "scalacheck" % "1.12.2" % "test",
  "commons-io" % "commons-io" % "2.4" % "test"
)

parallelExecution in Test := false

testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-o", "-u", "target/test-reports")

test in Test <<= (test in Test).dependsOn(pack in core)

libraryDependencies += "org.apache.commons" % "commons-exec" % "1.3" % "test"