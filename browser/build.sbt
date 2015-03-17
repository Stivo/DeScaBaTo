

name := "browser"

organization := "ch.descabato"

version := Common.version

scalaVersion := Common.scalaVersion

mainClass := Some("ch.descabato.browser.Main")

unmanagedSourceDirectories in Compile <++= baseDirectory { base =>
  Seq(
    base / "src/main/resources"
  )
}

libraryDependencies ++= Seq(
		"org.apache.commons" % "commons-vfs2" % "2.0"
//		"commons-httpclient" % "commons-httpclient" % "3.1",
//		"commons-net" % "commons-net" % "3.3"
)

scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature", "-Xlint")

// Dependencies for the otros vfs browser
libraryDependencies ++= Seq(
	"commons-configuration" % "commons-configuration" % "1.10",
	"commons-io" % "commons-io" % "2.4",
	"com.miglayout" % "miglayout-swing" % "5.0",
	"org.swinglabs.swingx" % "swingx-all" % "1.6.5-1",
	"net.java.dev.jgoodies" % "looks" % "2.1.4",
	"com.intellij" % "annotations" % "12.0",
	"org.ocpsoft.prettytime" % "prettytime" % "3.2.7.Final",
	"com.github.insubstantial" % "substance" % "7.3"
)

packSettings

packMain := Map("descabato" -> "ch.descabato.browser.Main")

packJvmOpts := Map("descabato" -> Seq("-Xms100m", "-Xmx500m"))

packJarNameConvention := "original"

packResourceDir := List("../doc/releases/0.4.1")