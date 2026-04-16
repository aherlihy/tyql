package tyql

import language.experimental.namedTuples
import scala.language.implicitConversions
import NamedTuple.*

type Edge = (x: Int, y: Int)
val edges = Table[Edge]("edges")

val tcQuery = edges.restrictedFix(path =>
  path
    .flatMap(p => edges.filter(e => p.y == e.x).map(e => (x = p.x, y = e.y).toRow))
    .distinct
)

@main def run(): Unit =
  println("TC: " + tcQuery.toSQLString)
