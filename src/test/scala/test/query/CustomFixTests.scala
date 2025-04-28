package test.query.customfix
import test.{SQLStringQueryTest, TestDatabase}
import tyql.*
import Query.{customFix, fix, unrestrictedFix}
import test.query.recursive.{Edge, TCDB, TCDBs}
import Expr.{IntLit, StringLit, count, max, min, sum}
import test.query.recursivebenchmarks.{WeightedGraphDB, WeightedGraphDBs}

import language.experimental.namedTuples
import NamedTuple.*
import scala.language.implicitConversions
class CustomFixBag extends SQLStringQueryTest[TCDB, Edge] {
  def testDescription: String = "Recursive query defined over bag, using unionAll using customFix"

  def query() =
    val path = testDB.tables.edges
    path.customFix((RestrictedConstructors(), Monotone(), BagResult(), Linear()))(pathRec =>
      pathRec.flatMap(p =>
        testDB.tables.edges
          .filter(e => p.y == e.x)
          .map(e => (x = e.y, y = e.y).toRow)
      ).unionAll(testDB.tables.edges)
    )

  def expectedQueryPattern: String =
    """
    WITH RECURSIVE recursive$436 AS ((SELECT edges$436.y as x, edges$436.y as y FROM recursive$436 as ref$244, edges as edges$436 WHERE ref$244.y = edges$436.x) UNION ALL ((SELECT * FROM edges as edges$439) UNION ALL (SELECT * FROM edges as edges$442))) SELECT * FROM recursive$436 as recref$16
    """
}

class CustomFixBagNeg extends munit.FunSuite {
  def testDescription: String = "recursive query defined over bag, using unionAll"
  def expectedError: String = """tyql.RestrictedQuery[(x : Int, y : Int), tyql.BagResult, Tuple1[(0 : Int)],
                                |  tyql.RestrictedConstructors, tyql.Monotone]
                                |Required: tyql.RestrictedQuery[Edge, tyql.SetResult, Tuple1[(0 : Int)],
                                |  tyql.RestrictedConstructors, tyql.Monotone]
                                |            ).unionAll(tables.edges2)""".stripMargin

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
          val path = tables.edges
          path.customFix((RestrictedConstructors(), Monotone(), SetResult(), Linear()))(pathRec =>
            pathRec.flatMap(p =>
              tables.edges
                .filter(e => p.y == e.x)
                .map(e => (x = e.y, y = e.y).toRow)
            ).unionAll(tables.edges2)
          )
          """)
    assert(error.contains(expectedError), s"Expected substring '$expectedError' in '$error'")
  }
}

class CustomFixConstructorNeg extends munit.FunSuite {
  def testDescription: String = "Single source shortest path fails due to restricted constructor preventing arithmetic in customFix"

  def expectedError: String =
    "value + is not a member of tyql.Expr[Int, tyql.NonScalarExpr, tyql.RestrictedConstructors]"

  test(testDescription) {
    val error: String =
      compileErrors(
        """
           import language.experimental.namedTuples
           import tyql.{Table, Expr, Query}
           import Query.customFix

           type WeightedEdge = (src: Int, dst: Int, cost: Int)
           type WeightedGraphDB = (edge: WeightedEdge, base: (dst: Int, cost: Int))

           val edge = Table[WeightedEdge]("edge")
           val base = Table[(dst: Int, cost: Int)]("base")

           base.customFix((RestrictedConstructors(), Monotone(), SetResult(), Linear()))(sp =>
             edge.flatMap(edge =>
               sp
                 .filter(s => s.dst == edge.src)
                 .map(s => (dst = edge.dst, cost = s.cost + edge.cost).toRow) // Should fail due to constructor restriction
             ).distinct
           ).aggregate(s => (dst = s.dst, cost = min(s.cost)).toGroupingRow)
            .groupBySource(s => (dst = s._1.dst).toRow)
        """)
    assert(error.contains(expectedError), s"Expected substring '$expectedError' in '$error'")
  }
}

class CustomFixMonotonicNeg extends munit.FunSuite {
  def testDescription: String = "Aggregation within recursive definition."
  def expectedError: String = "value aggregate is not a member of tyql.RestrictedQueryRef"

  test(testDescription) {
    val error: String =
      compileErrors(
        """
           // BOILERPLATE
           import language.experimental.namedTuples
           import tyql.{Table, Expr, Query}
           import Expr.{sum}
           import Query.customFix

           type Edge = (x: Int, y: Int)
           val table = Table[Edge]("edges")

          // TEST
          val path = table
          path.customFix((RestrictedConstructors(), Monotone(), SetResult(), Linear()))(pathRec =>
            pathRec.aggregate(p => (x = sum(p.x), y = sum(p.y)).toRow).distinct
          )
          """)
    assert(error.contains(expectedError), s"Expected substring '$expectedError' in '$error'")
  }
}

class CustomFixMonotonicTestPos extends munit.FunSuite {
  test("Aggregation within recursive definition under monotonic constraints using customFix"){
    type Edge = (x: Int, y: Int)
    val path = Table[Edge]("edges")

    intercept[NotImplementedError] {
      path.customFix((RestrictedConstructors(), NonMonotone(), SetResult(), Linear()))(pathRec =>
        pathRec.aggregate(p => (x = sum(p.x), y = sum(p.y)).toRow).distinct
      )
    }
  }
}

class CustomFixLinearTest extends SQLStringQueryTest[TCDB, Int] {
  def testDescription: String = "Linear recursion"

  def query() =
    val path = testDB.tables.edges
    path.customFix((RestrictedConstructors(), Monotone(), SetResult(), Linear()))(path =>
      path.flatMap(p =>
        testDB.tables.edges
          .filter(e => p.y == e.x)
          .map(e => (x = p.x, y = e.y).toRow)
      ).distinct
    ).map(p => p.x)

  def expectedQueryPattern: String =
    """
      WITH RECURSIVE recursive$A AS
        ((SELECT * FROM edges as edges$B)
          UNION
        ((SELECT ref$D.x as x, edges$C.y as y
        FROM recursive$A as ref$D, edges as edges$C
        WHERE ref$D.y = edges$C.x))) SELECT recref$E.x FROM recursive$A as recref$E
        """
}

class CustomFixRelevantNeg extends SQLStringQueryTest[TCDB, Edge] {
  def testDescription: String = "Linear recursion: relevant (skip pathToA)"

  def query() =
    val path = testDB.tables.edges
    val pathBase = testDB.tables.edges
    val (pathResult, pathToResult) = customFix((RestrictedConstructors(), Monotone(), SetResult(), NonLinear()))(pathBase, pathBase)((path, pathToA) =>
//    val (pathResult, pathToResult) = unrestrictedFix(pathBase, pathBase)((path, pathToA) =>
//    val (pathResult, pathToResult) = nonLinearFix(pathBase, pathBase)((path, pathToA) =>
      val P = path.flatMap(p => // deps = (0)
        testDB.tables.edges
          .filter(e => p.y == e.x)
          .map(e => (x = p.x, y = e.y).toRow)
      )
      val PtoA = path.filter(e => e.x == 1) // deps = (0)
      (P.distinct, PtoA.distinct)
    )
    pathResult

  def expectedQueryPattern: String =
    """
      WITH RECURSIVE
      recursive$1 AS
        ((SELECT * FROM edges as edges$2)
          UNION
        ((SELECT ref$0.x as x, edges$4.y as y FROM recursive$1 as ref$0, edges as edges$4 WHERE ref$0.y = edges$4.x))),
      recursive$2 AS
        ((SELECT * FROM edges as edges$8)
          UNION
         ((SELECT * FROM recursive$1 as ref$3 WHERE ref$3.x = 1))) SELECT * FROM recursive$1 as recref$0
        """
}

class CustomFixAffineNeg extends SQLStringQueryTest[TCDB, Edge] {
  def testDescription: String = "Linear recursion: affine (double path)"

  def query() =
    val path = testDB.tables.edges
    val pathBase = testDB.tables.edges
//    val (pathResult, pathToResult) = unrestrictedFix(pathBase, pathBase)((path, pathToA) =>
//    val (pathResult, pathToResult) = customFix((RestrictedConstructors(), Monotone(), SetResult(), NonLinear()))(pathBase, pathBase)((path, pathToA) =>
    val (pathResult, pathToResult) = customFix((RestrictedConstructors(), Monotone(), SetResult(), NonLinear()))(pathBase, pathBase)((path, pathToA) =>
      val P = path.flatMap(p =>
        path
          .filter(e => p.y == e.x)
          .map(e => (x = p.x, y = e.y).toRow)
      )
      val PtoA = path.filter(e => e.x == 1)
      (P.distinct, PtoA.distinct)
    )
    pathResult

  def expectedQueryPattern: String =
    """
      WITH RECURSIVE
      recursive$1 AS
        ((SELECT * FROM edges as edges$2)
          UNION
        ((SELECT ref$0.x as x, ref$4.y as y FROM recursive$1 as ref$0, recursive$1 as ref$4 WHERE ref$0.y = ref$4.x))),
      recursive$2 AS
        ((SELECT * FROM edges as edges$8)
          UNION
         ((SELECT * FROM recursive$1 as ref$3 WHERE ref$3.x = 1))) SELECT * FROM recursive$1 as recref$0
        """
}