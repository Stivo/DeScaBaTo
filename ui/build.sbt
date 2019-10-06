
name := "ui"

mainClass := Some("ch.descabato.ui.Main")

unmanagedSourceDirectories in Compile += baseDirectory.value / "src/main/resources"

resourceDirectory in Compile := (scalaSource in Compile).value

scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature", "-Xlint", "-Ymacro-annotations")

// Add dependency on ScalaFX library
libraryDependencies += "org.scalafx" %% "scalafx" % "12.0.2-R18"

libraryDependencies += "org.scalafx" %% "scalafxml-core-sfx8" % "0.5"

// Determine OS version of JavaFX binaries
lazy val osName = System.getProperty("os.name") match {
  case n if n.startsWith("Linux")   => "linux"
  case n if n.startsWith("Mac")     => "mac"
  case n if n.startsWith("Windows") => "win"
  case _ => throw new Exception("Unknown platform!")
}

lazy val javaFXModules = Seq("base", "controls", "fxml", "graphics", "media", "swing", "web")

libraryDependencies ++= javaFXModules.map( m =>
  "org.openjfx" % s"javafx-$m" % "12.0.2" classifier osName
)

fork := true

enablePlugins(PackPlugin)

packMain := Map("descabato" -> "ch.descabato.frontend.CLI")

packJvmOpts := Map("descabato" -> Seq("-Xms1g", "-Xmx2g"))

packJarNameConvention := "full"

packArchivePrefix := "descabato"

packResourceDir += (baseDirectory.value / "../README.md" -> "README.md")