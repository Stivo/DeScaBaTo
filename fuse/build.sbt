
name := "fuse"

resolvers += Resolver.jcenterRepo

scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature", "-Xlint")

libraryDependencies += "com.github.serceman" % "jnr-fuse" % "0.5.7"

fork := true

enablePlugins(PackPlugin)

packMain := Map("descabato" -> "ch.descabato.Main")

packJvmOpts := Map("descabato" -> Seq("-Xmx2g", "-Xms1g", "-XX:NewSize=1g", "-XX:MaxNewSize=1g"))

packJarNameConvention := "full"

packArchivePrefix := "descabato"

packResourceDir += (baseDirectory.value / "../README.md" -> "README.md")
