
name := "fuse"

mainClass := Some("ch.descabato.rocks.Main")

resolvers += Resolver.jcenterRepo

scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature", "-Xlint")

libraryDependencies += "com.github.serceman" % "jnr-fuse" % "0.5.4"

libraryDependencies += "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.10.3"

fork := true

enablePlugins(PackPlugin)

packMain := Map("descabato" -> "ch.descabato.rocks.Main")

packJvmOpts := Map("descabato" -> Seq("-Xms1g", "-Xmx2g"))

packJarNameConvention := "full"

packArchivePrefix := "descabato"

packResourceDir += (baseDirectory.value / "../README.md" -> "README.md")
