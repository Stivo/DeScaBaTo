
name := "ftp"

mainClass := Some("ch.descabato.rocks.Main")

scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature", "-Xlint")

// https://mvnrepository.com/artifact/org.apache.ftpserver/ftpserver-core
libraryDependencies += "org.apache.ftpserver" % "ftpserver-core" % "1.1.1"

fork := true

enablePlugins(PackPlugin)

packMain := Map("descabato" -> "ch.descabato.rocks.Main")

packJvmOpts := Map("descabato" -> Seq("-Xms1g", "-Xmx2g"))

packJarNameConvention := "full"

packArchivePrefix := "descabato"

packResourceDir += (baseDirectory.value / "../README.md" -> "README.md")
