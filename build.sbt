ThisBuild / version := "0.1.0-SNAPSHOT"

inThisBuild(Seq(
  organization := "ch.epfl.lamp",
  scalaVersion := "3.5.0-RC1-bin-SNAPSHOT",
  version := "0.0.1",
))

scalacOptions ++= Seq(
  "-experimental",
  "-feature",
  "-explain"
)

libraryDependencies ++= Seq(
  "org.scalameta" %% "munit" % "1.0.0" % Test,
)

lazy val root = (project in file("."))
  .settings(
    name := "tyql",
    Test / parallelExecution := false,
//    Test / testOptions += Tests.Argument(TestFrameworks.MUnit, "-b")

)