
name := "web"

resolvers += Resolver.jcenterRepo

scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature", "-Xlint")

libraryDependencies += "io.javalin" % "javalin" % "6.5.0"

libraryDependencies += "org.slf4j" % "slf4j-simple" % "2.0.17"

libraryDependencies += "com.fasterxml.jackson.datatype" % "jackson-datatype-jsr310" % "2.13.5"

enablePlugins(PackPlugin)

packMain := Map("descabato" -> "ch.descabato.Main")

packJvmOpts := Map("descabato" -> Seq("-Xmx2g", "-Xms1g", "-XX:NewSize=1g", "-XX:MaxNewSize=1g", "-Dfile.encoding=UTF-8"))

packJarNameConvention := "full"

packArchivePrefix := "descabato"

packResourceDir += (baseDirectory.value / "../README.md" -> "README.md")
