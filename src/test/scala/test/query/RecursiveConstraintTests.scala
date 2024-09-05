package test.query.recursiveconstraints

import test.{SQLStringQueryTest, TestDatabase}
import tyql.Table
import tyql.Expr.sum
import language.experimental.namedTuples

type Edge = (x: Int, y: Int)
type TCDB = (edges: Edge, edges2: Edge, emptyEdges: Edge)

given TCDBs: TestDatabase[TCDB] with
  override def tables = (
    edges = Table[Edge]("edges"),
    edges2 = Table[Edge]("edges2"),
    emptyEdges = Table[Edge]("empty")
  )

/**
 * Constraint: "safe" datalog queries, e.g. all variables present in the head
 * are also present in at least one body rule. Also called "range restricted".
 *
 * This is equivalent to saying that every column of the relation needs to be defined
 * in the recursive definition. This is handled by regular type checking the named tuple,
 * since if you try to assign (y: Int) to (x: Int, y: Int) it will not compile.
 */
class RecursionConstraintRangeRestrictionTest extends SQLStringQueryTest[TCDB, Edge] {
  def testDescription: String = "Range restricted correctly"

  def query() =
    val path = testDB.tables.edges
    path.fix(path =>
      path.flatMap(p =>
        testDB.tables.edges
          .filter(e => p.y == e.x)
          .map(e => (x = e.y, y = e.y).toRow)
      ).distinct
    )
  def expectedQueryPattern: String =
    """
    WITH RECURSIVE recursive$A AS
      ((SELECT * FROM edges as edges$B)
        UNION
      ((SELECT edges$C.y as x, edges$C.y as y
      FROM recursive$A as ref$D, edges as edges$C
      WHERE ref$D.y = edges$C.x))) SELECT * FROM recursive$A as recref$E
      """
}
class RecursionConstraintRangeRestrictionFailTest extends munit.FunSuite {
  def testDescription: String = "Range restricted incorrectly"
  def expectedError: String = "Found:    tyql.RestrictedQuery[(y : Int), tyql.SetResult]\nRequired: tyql.RestrictedQuery[Edge, tyql.SetResult]"

  test(testDescription) {
    val error: String =
      compileErrors(
        """
           // BOILERPLATE
           import language.experimental.namedTuples
           import tyql.{Table, Expr, Query}

           type Edge = (x: Int, y: Int)
           type TCDB = (edges: Edge, emptyEdges: Edge)

           val tables = (
             edges = Table[Edge]("edges"),
             emptyEdges = Table[Edge]("empty")
           )

          // TEST
          val path = tables.edges
          path.fix(path =>
            path.flatMap(p =>
              tables.edges
                .filter(e => p.y == e.x)
                .map(e => (y = e.y).toRow)
            ).distinct
          )
          """)
    assert(error.contains(expectedError), s"Expected substring '$expectedError' in '$error'")
  }
}
class RecursionConstraintCategoryResultTest extends SQLStringQueryTest[TCDB, Edge] {
  def testDescription: String = "recursive query defined over sets"

  def query() =
    val path = testDB.tables.edges
    path.fix(path =>
      path.flatMap(p =>
        testDB.tables.edges
          .filter(e => p.y == e.x)
          .map(e => (x = e.y, y = e.y).toRow)
      ).union(testDB.tables.edges2)
    )
  def expectedQueryPattern: String =
    """
    WITH RECURSIVE recursive$A AS
      ((SELECT * FROM edges as edges$B)
        UNION
      ((SELECT edges$C.y as x, edges$C.y as y
      FROM recursive$A as ref$D, edges as edges$C
      WHERE ref$D.y = edges$C.x)
        UNION
      (SELECT * FROM edges2 as edges2$F))) SELECT * FROM recursive$A as recref$E
      """
}
class RecursionConstraintCategory1FailTest extends munit.FunSuite {
  def testDescription: String = "recursive query defined over bag, using unionAll"
  def expectedError: String = "Found:    tyql.RestrictedQuery[(x : Int, y : Int), tyql.BagResult]\nRequired: tyql.RestrictedQuery[Edge, tyql.SetResult]"

  test(testDescription) {
    val error: String =
      compileErrors(
        """
           // BOILERPLATE
           import language.experimental.namedTuples
           import tyql.{Table, Expr, Query}

           type Edge = (x: Int, y: Int)
           type TCDB = (edges: Edge, edges2: Edge, emptyEdges: Edge)

           val tables = (
             edges = Table[Edge]("edges"),
             edges2 = Table[Edge]("edges2"),
             emptyEdges = Table[Edge]("empty")
           )

          // TEST
          val path = tables.edges
          path.fix(path =>
            path.flatMap(p =>
              tables.edges
                .filter(e => p.y == e.x)
                .map(e => (x = e.y, y = e.y).toRow)
            ).unionAll(tables.edges2)
          )
          """)
    assert(error.contains(expectedError), s"Expected substring '$expectedError' in '$error'")
  }
}
class RecursionConstraintCategory2FailTest extends munit.FunSuite {
  def testDescription: String = "recursive query defined over bag, missing distinct"
  def expectedError: String = "Found:    tyql.RestrictedQuery[(x : Int, y : Int), tyql.BagResult]\nRequired: tyql.RestrictedQuery[Edge, tyql.SetResult]"

  test(testDescription) {
    val error: String =
      compileErrors(
        """
           // BOILERPLATE
           import language.experimental.namedTuples
           import tyql.{Table, Expr, Query}

           type Edge = (x: Int, y: Int)
           type TCDB = (edges: Edge, edges2: Edge, emptyEdges: Edge)

           val tables = (
             edges = Table[Edge]("edges"),
             edges2 = Table[Edge]("edges2"),
             emptyEdges = Table[Edge]("empty")
           )

          // TEST
          val path = tables.edges
          path.fix(path =>
            path.flatMap(p =>
              tables.edges
                .filter(e => p.y == e.x)
                .map(e => (x = e.y, y = e.y).toRow)
            )
          )
          """)
    assert(error.contains(expectedError), s"Expected substring '$expectedError' in '$error'")
  }
}
class RecursionConstraintMonotonic1FailTest extends munit.FunSuite {
  def testDescription: String = "Aggregation within recursive definition"
  def expectedError: String = "value aggregate is not a member of tyql.Query.RestrictedQueryRef"

  test(testDescription) {
    val error: String =
      compileErrors(
        """
           // BOILERPLATE
           import language.experimental.namedTuples
           import tyql.{Table, Expr, Query}

           type Edge = (x: Int, y: Int)
           type Edge2 = (z: Int, q: Int)
           type TCDB = (edges: Edge, otherEdges: Edge2, emptyEdges: Edge)

           val tables = (
             edges = Table[Edge]("edges"),
             otherEdges = Table[Edge2]("otherEdges"),
             emptyEdges = Table[Edge]("empty")
           )

          // TEST
          val pathBase = tables.edges
          val pathToABase = tables.emptyEdges
          val (pathResult, pathToAResult) = Query.fix(pathBase, pathToABase)((path, pathToA) =>
            val P = path.flatMap(p =>
              tables.edges
                .filter(e => p.y == e.x)
                .map(e => (x = p.x, y = e.y).toRow)
            )
            val PtoA = path.aggregate(e => Expr.Avg(e.x) == "A")
            (P, PtoA)
          )
          """)
    assert(error.contains(expectedError), s"Expected substring '$expectedError' in '$error'")
  }
}
class RecursionConstraintMonotonic2FailTest extends munit.FunSuite {
  def testDescription: String = "Aggregation within recursive definition using query-level agg"
  def expectedError: String = "value size is not a member of tyql.Query.RestrictedQueryRef"

  test(testDescription) {
    val error: String =
      compileErrors(
        """
           // BOILERPLATE
           import language.experimental.namedTuples
           import tyql.{Table, Expr, Query}

           type Edge = (x: Int, y: Int)
           type Edge2 = (z: Int, q: Int)
           type TCDB = (edges: Edge, otherEdges: Edge2, emptyEdges: Edge)

           val tables = (
             edges = Table[Edge]("edges"),
             otherEdges = Table[Edge2]("otherEdges"),
             emptyEdges = Table[Edge]("empty")
           )

          // TEST
          val pathBase = tables.edges
          val pathToABase = tables.emptyEdges
          val (pathResult, pathToAResult) = Query.fix(pathBase, pathToABase)((path, pathToA) =>
            val P = path.flatMap(p =>
              tables.edges
                .filter(e => p.y == e.x)
                .map(e => (x = p.x, y = e.y).toRow)
            ).distinct
            val PtoA = path.size()
            (P, PtoA)
          )
          """)
    assert(error.contains(expectedError), s"Expected substring '$expectedError' in '$error'")
  }
}
class RecursionConstraintMonotonic3FailTest extends munit.FunSuite {
  def testDescription: String = "Aggregation within inline fix"
  def expectedError: String = "value aggregate is not a member of tyql.Query.RestrictedQueryRef"

  test(testDescription) {
    val error: String =
      compileErrors(
        """
           // BOILERPLATE
           import language.experimental.namedTuples
           import tyql.{Table, Expr}

           type Edge = (x: Int, y: Int)
           type Edge2 = (z: Int, q: Int)
           type TCDB = (edges: Edge, otherEdges: Edge2, emptyEdges: Edge)

           val tables = (
             edges = Table[Edge]("edges"),
             otherEdges = Table[Edge2]("otherEdges"),
             emptyEdges = Table[Edge]("empty")
           )

          // TEST
          val path = tables.edges
          path.fix(path =>
            path.aggregate(p =>
              tables.edges
                .filter(e => p.y == e.x)
                .aggregate(e => (x = Expr.avg(p.x), y = Expr.sum(e.y)).toRow)
            )
          )
          """)
    assert(error.contains(expectedError), s"Expected substring '$expectedError' in '$error'")
  }
}
