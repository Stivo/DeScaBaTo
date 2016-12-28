
name := "browser"

mainClass := Some("ch.descabato.browser.Main")

unmanagedSourceDirectories in Compile <++= baseDirectory { base =>
  Seq(
    base / "src/main/resources"
  )
}

scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature", "-Xlint")

libraryDependencies ++= Seq(
	"org.webjars.bower" % "angular-filemanager" % "1.5.1",
	"com.typesafe.akka" %% "akka-http" % "10.0.1",
  "com.typesafe.akka" %% "akka-http-spray-json" % "10.0.1",
  "org.webjars" % "webjars-locator-core" % "0.32",
  "org.scalafx" %% "scalafx" % "8.0.102-R11",
  "org.scalafx" %% "scalafxml-core-sfx8" % "0.3"
)

addCompilerPlugin("org.scalamacros" % "paradise" % "2.1.0" cross CrossVersion.full)

packSettings

packMain := Map("descabato" -> "ch.descabato.web.Main")

packJvmOpts := Map("descabato" -> Seq("-Xms100m", "-Xmx500m"))

packJarNameConvention := "full"

packArchivePrefix := "descabato"

