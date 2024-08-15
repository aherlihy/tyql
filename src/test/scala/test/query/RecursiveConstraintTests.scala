package test.query.recursiveconstraints

import test.{SQLStringQueryTest, TestDatabase}
import tyql.Table
import tyql.Expr.sum
import language.experimental.namedTuples

type Edge = (x: Int, y: Int)
type Edge2 = (z: Int, q: Int)
type TCDB = (edges: Edge, otherEdges: Edge2, emptyEdges: Edge)

given TCDBs: TestDatabase[TCDB] with
  override def tables = (
    edges = Table[Edge]("edges"),
    otherEdges = Table[Edge2]("otherEdges"),
    emptyEdges = Table[Edge]("empty")
  )

/**
 * Constraint: "safe"/monotonic datalog queries, e.g. all variables present in the head
 * are also present in at least one body rule. Also called "range restricted"
 */


class RecursionConstraint1Test extends SQLStringQueryTest[TCDB, Edge] {
  def testDescription: String = "Range restrictions: no elements of path is returned in the result, BUT, is used in the filter"

  def query() =
    val path = testDB.tables.edges
    path.fix(path =>
      path.flatMap(p =>
        testDB.tables.edges
          .filter(e => p.y == e.x)
          .map(e => (x = e.y, y = e.y).toRow)
      )
    )
  def expectedQueryPattern: String =
    """
    WITH RECURSIVE recursive$A AS
      (SELECT * FROM edges as edges$B
        UNION ALL
      SELECT edges$C.y as x, edges$C.y as y
      FROM recursive$A as ref$D, edges as edges$C
      WHERE ref$D.y = edges$C.x) SELECT * FROM recursive$A as recref$E
      """
}

class RecursionConstraint2Test extends SQLStringQueryTest[TCDB, Edge] {
  def testDescription: String = "Range restrictions: no elements of path is returned in the result OR used in the filter"

  def query() =
    val path0 = testDB.tables.edges
    path0.fix(path => // wrap path in a different API so that by construction you can't call methods that would allow a cycle
      path.flatMap(p =>
        testDB.tables.edges
          .filter(e => e.x == "A")
          .map(e => (x = e.y, y = e.y).toRow)
      )
    )
  def expectedQueryPattern: String =
    """
    WITH RECURSIVE recursive$A AS
      (SELECT * FROM edges as edges$B
        UNION ALL
      SELECT edges$C.y as x, edges$C.y as y
      FROM recursive$A as ref$D, edges as edges$C
      WHERE edges$C.x = "A") SELECT * FROM recursive$A as recref$E
      """
}

class RecursionConstraint3Test extends SQLStringQueryTest[TCDB, Edge] {
  def testDescription: String = "Stratified aggregation: should not be able to call aggregate on recursive definition"

  def query() =
    val path0 = testDB.tables.edges
    path0.fix(path => // wrap path in a different API so that by construction you can't call methods that would allow a cycle
      path.flatMap(p =>
        testDB.tables.edges
          .filter(e => e.x == "A")
          .map(e => (x = sum(p.y), y = e.y))
      )
    )
  def expectedQueryPattern: String =
    """
    WITH RECURSIVE recursive$A AS
      (SELECT * FROM edges as edges$B
        UNION ALL
      SELECT edges$C.y as x, edges$C.y as y
      FROM recursive$A as ref$D, edges as edges$C
      WHERE edges$C.x = "A") SELECT * FROM recursive$A as recref$E
      """
}