package test.query.recursiveconstraints

import test.{SQLStringQueryTest, TestDatabase}
import tyql.Table
import tyql.Expr.sum
import tyql.Query
import tyql.Query.fix
import scala.compiletime.summonInline
import scala.reflect.Typeable

import language.experimental.namedTuples

type Edge = (x: Int, y: Int)
type EdgeOther = (z: Int, q: Int)
type TCDB = (edges: Edge, edges2: Edge, otherEdges: EdgeOther, emptyEdges: Edge)

given TCDBs: TestDatabase[TCDB] with
  override def tables = (
    edges = Table[Edge]("edges"),
    edges2 = Table[Edge]("edges2"),
    otherEdges = Table[EdgeOther]("otherEdges"),
    emptyEdges = Table[Edge]("empty")
  )
/*
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
  def expectedError: String = "Found:    tyql.RestrictedQuery[(y : Int), tyql.SetResult, path.D]\nRequired: tyql.RestrictedQuery[Edge, tyql.SetResult, ?]"

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
  def expectedError: String = "Found:    tyql.RestrictedQuery[(x : Int, y : Int), tyql.BagResult, path.D]\nRequired: tyql.RestrictedQuery[Edge, tyql.SetResult, ?]"

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
  def expectedError: String = "Found:    tyql.RestrictedQuery[(x : Int, y : Int), tyql.BagResult, path.D]\nRequired: tyql.RestrictedQuery[Edge, tyql.SetResult, ?]"

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
  def expectedError: String = "value aggregate is not a member of tyql.RestrictedQueryRef"

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
  def expectedError: String = "value size is not a member of tyql.RestrictedQueryRef"

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
  def expectedError: String = "value aggregate is not a member of tyql.RestrictedQueryRef"

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

class RecursiveConstraintLinearTest extends SQLStringQueryTest[TCDB, Int] {
  def testDescription: String = "Linear recursion"

  def query() =
    val path = testDB.tables.edges
    path.fix(path =>
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

class RecursiveConstraintLinear2FailTest extends munit.FunSuite {
  def testDescription: String = "Non-linear recursion: 2 usages of path, inline fix"

  def expectedError: String = "Cannot prove that Tuple.Disjoint[path.D, path.D]"

  test(testDescription) {
    val error: String =
      compileErrors(
        """
               // BOILERPLATE
               import language.experimental.namedTuples
               import tyql.{Table, Expr}

               type Edge = (x: Int, y: Int)

               val tables = (
                 edges = Table[Edge]("edges"),
                 edges2 = Table[Edge]("otherEdges"),
                 emptyEdges = Table[Edge]("empty")
               )

              // TEST
              val path = tables.edges
               path.fix(path =>
                 path.flatMap(p =>
                  path
                    .filter(e => p.y == e.x)
                    .map(e => (x = p.x, y = e.y).toRow)
                ).distinct
              )
              """)
    assert(error.contains(expectedError), s"Expected substring '$expectedError' in '$error'")
  }
}

class RecursiveConstraintLinearFailTest extends munit.FunSuite {
  def testDescription: String = "Non-linear recursion: 0 usages of path, inline fix"

  def expectedError: String = "Found:    tyql.Query[(x : Int, y : Int), tyql.SetResult]\nRequired: tyql.RestrictedQuery[Edge, tyql.SetResult, ?]"

  test(testDescription) {
    val error: String =
      compileErrors(
        """
             // BOILERPLATE
             import language.experimental.namedTuples
             import tyql.{Table, Expr}

             type Edge = (x: Int, y: Int)

             val tables = (
               edges = Table[Edge]("edges"),
               edges2 = Table[Edge]("otherEdges"),
               emptyEdges = Table[Edge]("empty")
             )

            // TEST
            val path = tables.edges
            path.fix(path =>
              tables.edges.flatMap(p =>
                tables.edges2
                  .filter(e => p.y == e.x)
                  .map(e => (x = p.x, y = e.y).toRow)
              ).distinct
            )
            """)
    assert(error.contains(expectedError), s"Expected substring '$expectedError' in '$error'")
  }
}
*/
class RecursiveConstraintLinear3FailTest extends munit.FunSuite {
  def testDescription: String = "Non-linear recursion: multiple uses of path in multifix"

  def expectedError: String = "Cannot prove that Tuple.Disjoint[Tuple1[(0 : Int)], Tuple1[(0 : Int)]] =:= (true : Boolean)"

  test(testDescription) {
    val error: String =
      compileErrors(
        """
             // BOILERPLATE
             import language.experimental.namedTuples
             import tyql.{Table, Expr}

             type Edge = (x: Int, y: Int)

             val tables = (
               edges = Table[Edge]("edges"),
               edges2 = Table[Edge]("otherEdges"),
               emptyEdges = Table[Edge]("empty")
             )

            // TEST
              val pathBase = tables.edges
              val path2Base = tables.emptyEdges
              val (pathResult, path2Result) = fix(pathBase, path2Base)((path, path2) =>
                val P = path.flatMap(p =>
                 path
                    .filter(e => p.y == e.x)
                    .map(e => (x = p.x, y = e.y).toRow)
                ).distinct
                val P2 = path2.flatMap(p =>
                  path2
                    .filter(e => p.y == e.x)
                    .map(e => (x = p.x, y = e.y).toRow)
                ).distinct
                (P, P2)
              )
            """)
    assert(error.contains(expectedError), s"Expected substring '$expectedError' in '$error'")
  }
}

class RecursiveConstraintLinear4FailTest extends munit.FunSuite {
  def testDescription: String = "Non-linear recursion: zero usage of path in multifix"

  def expectedError: String = "Cannot prove that tyql.Query.ExpectedResult[(tyql.Query[Edge, ?], tyql.Query[Edge, ?])] <:< tyql.Query.ActualResult["

  test(testDescription) {
    val error: String =
      compileErrors(
        """
               // BOILERPLATE
               import language.experimental.namedTuples
               import tyql.{Table, Expr}

               type Edge = (x: Int, y: Int)

               val tables = (
                 edges = Table[Edge]("edges"),
                 edges2 = Table[Edge]("otherEdges"),
                 emptyEdges = Table[Edge]("empty")
               )

              // TEST
                val pathBase = tables.edges
                val pathToABase = tables.emptyEdges
                val (pathResult, pathToAResult) = fix[(Query[Edge, ?], Query[Edge, ?]), (Tuple1[0], Tuple1[0])](pathBase, pathToABase)((path, pathToA) =>
                  val P = path.flatMap(p =>
                    testDB.tables.edges
                      .filter(e => p.y == e.x)
                      .map(e => (x = p.x, y = e.y).toRow)
                  )
                  val PtoA = path.filter(e => e.x == 1)
                  (P.distinct, PtoA.distinct)
                )

                pathToAResult
                  )
              """)
    assert(error.contains(expectedError), s"Expected substring '$expectedError' in '$error'")
  }
}

// TODO: this should fail
//class RecursiveConstraintTempFailTest extends SQLStringQueryTest[TCDB, Edge] {
//  def testDescription: String = "Non-linear recursion: 0 usages of path, multifix"
//
//  def query() =
//    val pathBase = testDB.tables.edges
//    val pathToABase = testDB.tables.emptyEdges
////    val (pathResult, pathToAResult) = fix(pathBase, pathToABase)((path, pathToA) =>
//    val (pathResult, pathToAResult) = fix[(Query[Edge, ?], Query[Edge, ?]), (Tuple1[0], Tuple1[0])](pathBase, pathToABase)((path, pathToA) =>
//      val P = path.flatMap(p =>
//        testDB.tables.edges
//          .filter(e => p.y == e.x)
//          .map(e => (x = p.x, y = e.y).toRow)
//      )
//      val PtoA = path.filter(e => e.x == 1)
//      (P.distinct, PtoA.distinct)
//    )
//
//    pathToAResult
//
//  def expectedQueryPattern: String =
//    """
//      WITH RECURSIVE
//          recursive$P AS
//            ((SELECT * FROM edges as edges$F)
//                UNION
//             ((SELECT ref$Z.x as x, edges$C.y as y
//             FROM recursive$P as ref$Z, edges as edges$C
//             WHERE ref$Z.y = edges$C.x))),
//          recursive$A AS
//           ((SELECT * FROM empty as empty$D)
//              UNION
//            ((SELECT * FROM recursive$P as ref$X WHERE ref$X.x = 1)))
//      SELECT * FROM recursive$A as recref$Q
//      """
//}

class RecursiveConstraintLinear5Test extends SQLStringQueryTest[TCDB, Edge] {
  def testDescription: String = "Linear recursion: refs used more than once, but only once per definition, multifix"

  def query() =
    val pathBase = testDB.tables.edges
    val path2Base = testDB.tables.emptyEdges
    val (pathResult, path2Result) = fix[(Query[Edge, ?], Query[Edge, ?]), (Tuple1[0], (0, 1))](pathBase, path2Base)((path, path2) =>
//    val (pathResult, path2Result) = fix(pathBase, path2Base)((path, path2) =>
      val P = path.flatMap(p =>
        testDB.tables.edges
          .filter(e => p.y == e.x)
          .map(e => (x = p.x, y = e.y).toRow)
      ).distinct
      val P2 = path.flatMap(p =>
        path2
          .filter(e => p.y == e.x)
          .map(e => (x = p.x, y = e.y).toRow)
      ).distinct
      (P, P2)
    )

    path2Result

  def expectedQueryPattern: String =
    """
       WITH RECURSIVE
          recursive$13 AS
            ((SELECT * FROM edges as edges$14)
              UNION
            ((SELECT ref$3.x as x, edges$16.y as y FROM recursive$13 as ref$3, edges as edges$16 WHERE ref$3.y = edges$16.x))),
          recursive$14 AS
          ((SELECT * FROM empty as empty$20)
              UNION
           ((SELECT ref$6.x as x, ref$7.y as y FROM recursive$13 as ref$6, recursive$14 as ref$7 WHERE ref$6.y = ref$7.x)))
       SELECT * FROM recursive$14 as recref$2
      """
}
/*
class RecursiveConstraintLinear6Test extends SQLStringQueryTest[TCDB, Edge] {
  def testDescription: String = "Linear recursion: refs used more than once, but only once per definition, but with same row type, multifix"

  def query() =
    val pathBase = testDB.tables.edges
    val path2Base = testDB.tables.emptyEdges
    val (pathResult, path2Result) = fix(pathBase, path2Base)((path, path2) =>
      val P = path.union(path2)
      val P2 = path2.union(path)
      (P, P2)
    )

    path2Result

  def expectedQueryPattern: String =
    """
        WITH RECURSIVE
          recursive$1 AS
            ((SELECT * FROM edges as edges$2)
              UNION
             ((SELECT * FROM recursive$1 as recref$0)
                UNION
                (SELECT * FROM recursive$2 as recref$1))),
          recursive$2 AS
            ((SELECT * FROM empty as empty$8)
                UNION
              ((SELECT * FROM recursive$2 as recref$1)
                UNION
               (SELECT * FROM recursive$1 as recref$0)))
        SELECT * FROM recursive$2 as recref$1
      """
}

class RecursiveConstraintLinear7Test extends SQLStringQueryTest[TCDB, Edge] {
  def testDescription: String = "Linear recursion: refs used more than once, but only once per definition, but with different row type, multifix"

  def query() =
    val pathBase = testDB.tables.edges
    val path2Base = testDB.tables.otherEdges
    val (pathResult, path2Result) = fix(pathBase, path2Base)((path, path2) =>
      val P = path2.flatMap(p =>
        path
          .filter(e => p.q == e.x)
          .map(e => (x = p.q, y = e.y).toRow)
      ).distinct
      val P2 = path2
      (P, P2)
    )

    pathResult

  def expectedQueryPattern: String =
    """
        WITH RECURSIVE
          recursive$1 AS
            ((SELECT * FROM edges as edges$2)
              UNION
             ((SELECT ref$0.q as x, ref$1.y as y FROM recursive$2 as ref$0, recursive$1 as ref$1 WHERE ref$0.q = ref$1.x))),
          recursive$2 AS
            ((SELECT * FROM otherEdges as otherEdges$7)
                UNION
            ((SELECT * FROM recursive$2 as recref$1)))
        SELECT * FROM recursive$1 as recref$0
      """
}
*/
//class TESTTEST extends SQLStringQueryTest[TCDB, Int] {
//  def testDescription: String = "Live tests"
//
//  def query() =
//    val pathBase = testDB.tables.edges
//    val path2Base = testDB.tables.emptyEdges
//    val (pathResult, path2Result) = fix(pathBase, path2Base)((path, path2) =>
//      val P = path.flatMap(p =>
//        path
//          .filter(e => p.y == e.x)
//          .map(e => (x = p.x, y = e.y).toRow)
//      ).distinct
//      val P2 = path2.flatMap(p =>
//        path2
//          .filter(e => p.y == e.x)
//          .map(e => (x = p.x, y = e.y).toRow)
//      ).distinct
//      (P, P2)
//    )
//
//  def expectedQueryPattern: String =
//    """
//        """
//}