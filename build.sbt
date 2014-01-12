scalaVersion := "2.10.3"

mainClass := Some("backup.CommandLine")

packageArchetype.java_application

unmanagedSourceDirectories in Compile <++= baseDirectory { base =>
  Seq(
    base / "src/main/resources",
    base / "src/test/resources"
  )
}

libraryDependencies += "org.fusesource.jansi" % "jansi" % "1.11"

libraryDependencies ++= Seq(
		"ch.qos.logback" % "logback-classic" % "1.0.13",
		"com.typesafe" % "scalalogging-slf4j_2.10" % "1.0.1"
)

libraryDependencies ++= Seq(
		"com.fasterxml.jackson.module" % "jackson-module-scala_2.10" % "2.3.1",
		"com.fasterxml.jackson.dataformat" % "jackson-dataformat-smile" % "2.3.1"
)

libraryDependencies ++= Seq(
		"org.apache.commons" % "commons-vfs2" % "2.0",
		"commons-httpclient" % "commons-httpclient" % "3.1",
		"commons-net" % "commons-net" % "3.3"
)

libraryDependencies += "org.scalatest" %% "scalatest" % "2.0" % "test"

libraryDependencies += "org.scalacheck" %% "scalacheck" % "1.10.0" % "test"

libraryDependencies += "com.quantifind" %% "sumac" % "0.3-SNAPSHOT"

libraryDependencies += "org.apache.commons" % "commons-vfs2" % "2.0"

libraryDependencies += "com.typesafe.akka" %% "akka-actor" % "2.2.3"