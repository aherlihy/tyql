package test.query.recursive
import test.{SQLStringQueryTest, SQLStringAggregationTest, TestDatabase}

import tyql.*
import Query.fix
import language.experimental.namedTuples
import NamedTuple.*
import scala.language.implicitConversions

type Edge = (x: Int, y: Int)
type TCDB = (edges: Edge)

given TCDBs: TestDatabase[TCDB] with
  override def tables = (
    edges = Table[Edge]("edges")
  )

// Expression-based aggregation:
class Recursion1Test extends SQLStringQueryTest[TCDB, Edge] {
  def testDescription: String = "TC"

  def query() =
    val path = testDB.tables.edges
    path.fix(path =>
      path.flatMap(p =>
        testDB.tables.edges
          .filter(e => p.y == e.x)
          .map(e => (x = p.x, y = e.y).toRow)
      )
    )
  def expectedQueryPattern: String =
    """
    WITH RECURSIVE recursive$A AS
      (SELECT * FROM edges as edges$B
        UNION ALL
      SELECT recursive$A.x as x, edges$C.y as y
      FROM recursive$A, edges as edges$C
      WHERE recursive$A.y = edges$C.x); SELECT * FROM recursive0
      """
}
