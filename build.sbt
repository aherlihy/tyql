ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "3.5.0-RC1-bin-SNAPSHOT"
//scalaHome := Some(file("scalaex"))

lazy val root = (project in file("."))
  .settings(
    name := "tyql"
  )
scalacOptions ++= Seq(
  "-experimental",
)