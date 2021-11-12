
ThisBuild / scalaVersion := Common.scalaDefaultVersion

val commonSettings =
  List(
    artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
      s"${module.organization}.${artifact.name}-${module.revision}.${artifact.extension}"
    },
    organization := Common.organization,
    version := Common.version,
    scalaVersion := Common.scalaDefaultVersion,
    crossScalaVersions := Common.scalaCrossVersions
  )

val core = (project in file("core"))
  .settings(commonSettings)

val fuse = (project in file("fuse"))
  .dependsOn(core)
  .settings(commonSettings)

val it = (project in file("integrationtest"))
  .dependsOn(core % "test->test")
  .settings(commonSettings, name := "it",
    Test / test := ((Test / test).dependsOn(core / pack)).value
  )
