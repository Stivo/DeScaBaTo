import sbt._
import Keys._
import sbtassembly.Plugin._
import AssemblyKeys._

assemblySettings

mainClass in assembly := Some("backup.CommandLine")