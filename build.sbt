ThisBuild / version := "0.1.0-SNAPSHOT"

inThisBuild(Seq(
  organization := "ch.epfl.lamp",
  scalaVersion := "3.5.1-RC1",
  version := "0.0.1",
  libraryDependencies ++= Seq(
    "org.scalameta" %% "munit" % "1.0.0+24-ee555b1d-SNAPSHOT" % Test,
    "org.duckdb" % "duckdb_jdbc" % "1.1.1",
    "com.lihaoyi" %% "scalasql" % "0.1.11"
  )
))

scalacOptions ++= Seq(
  "-experimental",
  "-feature",
  "-explain"
)

//resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"
resolvers ++= Resolver.sonatypeOssRepos("snapshots")



lazy val root = (project in file("."))
  .enablePlugins(BuildInfoPlugin)
  .settings(
    name := "tyql",
    Test / parallelExecution := false,
//    Test / testOptions += Tests.Argument(TestFrameworks.MUnit, "-b")
    buildInfoKeys := Seq[BuildInfoKey](baseDirectory),
    buildInfoPackage := "buildinfo",
//    cleanFiles ++= Seq(
//      baseDirectory.value / "bench/data/ancestry/out/collections.csv",
//      baseDirectory.value / "bench/data/ancestry/out/tyql.csv",
//      baseDirectory.value / "bench/data/ancestry/out/scalasql.csv",
//      baseDirectory.value / "bench/data/andersens/out/collections.csv",
//      baseDirectory.value / "bench/data/andersens/out/tyql.csv",
//      baseDirectory.value / "bench/data/andersens/out/scalasql.csv",
//    ),
    cleanFiles ++= Seq("tc",
      "ancestry",
      "andersens",
      "asps",
      "bom",
      "cba",
      "cc",
      "cspa",
      "dataflow",
      "evenodd",
      "javapointsto",
      "orbits",
      "party",
      "pointstocount",
      "sssp",
      "tc",
      "trustchain",
    ).flatMap(bm => Seq("collections", "tyql", "scalasql")
        .map(ty =>
          baseDirectory.value / s"bench/data/$bm/out/$ty.csv"))
)
lazy val bench = (project in file("bench"))
  .dependsOn(root)
  .enablePlugins(JmhPlugin)
  .settings(
    Jmh/compile := (Jmh/compile).dependsOn(Test/compile).value,
    Jmh/run := (Jmh/run).dependsOn(Jmh/compile).evaluated,

    // sbt-jmh generates a ton of Java files, but they're never referenced by Scala files.
    // By enforcing this using `compileOrder`, we avoid having to run these generated files
    // through the Scala typechecker which has a significant impact on compile-time.
    Jmh/compileOrder := CompileOrder.ScalaThenJava

  )
