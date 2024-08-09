ThisBuild / version := "0.1.0-SNAPSHOT"

inThisBuild(Seq(
  organization := "ch.epfl.lamp",
  scalaVersion := "3.5.1-RC1",
  version := "0.0.1",
))

scalacOptions ++= Seq(
  "-experimental",
  "-feature",
  "-explain"
)

//resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"
resolvers ++= Resolver.sonatypeOssRepos("snapshots")

libraryDependencies ++= Seq(
  "org.scalameta" %% "munit" % "1.0.0+24-ee555b1d-SNAPSHOT" % Test,
)

lazy val root = (project in file("."))
  .settings(
    name := "tyql",
    Test / parallelExecution := false,
//    Test / testOptions += Tests.Argument(TestFrameworks.MUnit, "-b")

)
