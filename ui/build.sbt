
name := "ui"

mainClass := Some("ch.descabato.ui.Main")

unmanagedSourceDirectories in Compile += baseDirectory.value / "src/main/resources"

scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature", "-Xlint")

libraryDependencies ++= Seq(
  "org.scalafx" %% "scalafx" % "8.0.144-R12",
  "org.scalafx" %% "scalafxml-core-sfx8" % "0.4"
)

addCompilerPlugin("org.scalamacros" % "paradise" % "2.1.1" cross CrossVersion.full)

//packSettings
//
//packMain := Map("descabato" -> "ch.descabato.frontend.CLI")
//
//packJvmOpts := Map("descabato" -> Seq("-Xms100m", "-Xmx500m"))
//
//packJarNameConvention := "full"
//
//packArchivePrefix := "descabato"
//
