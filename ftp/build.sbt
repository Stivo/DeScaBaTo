
name := "ftp"

mainClass := Some("ch.descabato.rocks.Main")

resolvers += Resolver.jcenterRepo

scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature", "-Xlint")

// https://mvnrepository.com/artifact/org.apache.ftpserver/ftpserver-core
libraryDependencies += "org.apache.ftpserver" % "ftpserver-core" % "1.1.1"

libraryDependencies += "com.github.serceman" % "jnr-fuse" % "0.5.3"

fork := true

enablePlugins(PackPlugin)

packMain := Map("descabato" -> "ch.descabato.rocks.Main")

packJvmOpts := Map("descabato" -> Seq("-Xms1g", "-Xmx2g"))

packJarNameConvention := "full"

packArchivePrefix := "descabato"

packResourceDir += (baseDirectory.value / "../README.md" -> "README.md")
