
name := "fuse"

mainClass := Some("ch.descabato.rocks.Main")

resolvers += Resolver.jcenterRepo

scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature", "-Xlint")

libraryDependencies += "com.github.serceman" % "jnr-fuse" % "0.5.4"

libraryDependencies += "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.10.3"

fork := true

enablePlugins(PackPlugin)

packMain := Map("descabato" -> "ch.descabato.rocks.Main")

packJvmOpts := Map("descabato" -> Seq("-Xmx2g", "-Xms1g", "-XX:NewSize=1g", "-XX:MaxNewSize=1g", "-XX:MaxPermSize=1g"))

packJarNameConvention := "full"

packArchivePrefix := "descabato"

packResourceDir += (baseDirectory.value / "../README.md" -> "README.md")
