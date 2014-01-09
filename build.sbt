scalaVersion := "2.10.3"

libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.0.13"

libraryDependencies += "com.typesafe" % "scalalogging-slf4j_2.10" % "1.0.1"

libraryDependencies += "org.scalatest" % "scalatest_2.10" % "2.0" % "test"

libraryDependencies += ("com.twitter" %% "chill" % "0.3.5").exclude("com.esotericsoftware.minlog","minlog") 

libraryDependencies += "com.quantifind" %% "sumac" % "0.3-SNAPSHOT"
