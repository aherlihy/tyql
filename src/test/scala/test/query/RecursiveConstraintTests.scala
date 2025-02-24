package test.query.recursiveconstraints

import test.{SQLStringQueryTest, TestDatabase}
import tyql.{Query, RestrictedQuery, Table, SetResult}
import tyql.Expr.sum
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
  def expectedError: String = "Found:    tyql.RestrictedQuery[(y : Int), tyql.SetResult, Tuple1[(0 : Int)]]\nRequired: tyql.RestrictedQuery[Edge, tyql.SetResult, Tuple1[(0 : Int)]]"

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
class RecursionConstraintCategoryUnionAllFailTest extends munit.FunSuite {
  def testDescription: String = "recursive query defined over bag, using unionAll"
  def expectedError: String = "Found:    tyql.RestrictedQuery[(x : Int, y : Int), tyql.BagResult, Tuple1[(0 : Int)]]\nRequired: tyql.RestrictedQuery[Edge, tyql.SetResult, Tuple1[(0 : Int)]]"

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
class RecursionConstraintCategoryFlatmapFailTest extends munit.FunSuite {
  def testDescription: String = "recursive query defined over bag, missing distinct"
  def expectedError: String = "Found:    tyql.RestrictedQuery[(x : Int, y : Int), tyql.BagResult, Tuple1[(0 : Int)]]\nRequired: tyql.RestrictedQuery[Edge, tyql.SetResult, Tuple1[(0 : Int)]]"

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
class RecursionConstraintMonotonicInlineFailTest extends munit.FunSuite {
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

class RecursiveConstraintLinearFailInline0Test extends munit.FunSuite {
  def testDescription: String = "Non-linear recursion: 0 usages of path, inline fix"

  // Special because inline fix
  def expectedError: String = "Found:    tyql.Query[(x : Int, y : Int), tyql.SetResult]\nRequired: tyql.RestrictedQuery[Edge, tyql.SetResult, Tuple1[(0 : Int)]]"

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

// TODO: improve error messages for inline fix
class RecursiveConstraintLinearInline2xFailTest extends munit.FunSuite {
  def testDescription: String = "Non-linear recursion: 2 usages of path, inline fix"

//  def expectedError: String = "Recursive definition must be linearly recursive, e.g. each recursive reference cannot be used twice"
  def expectedError: String = "Found:    tyql.RestrictedQuery[(x : Int, y : Int), tyql.SetResult, ((0 : Int), (0 : Int))]\nRequired: tyql.RestrictedQuery[Edge, tyql.SetResult, Tuple1[(0 : Int)]]"
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

class RecursiveConstraintLinearMultifix2xFailTest extends munit.FunSuite {
  def testDescription: String = "Non-linear recursion: multiple uses of path in multifix"

  def expectedError: String = "Recursive definitions must be linear"

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

class RecursiveConstraintLinearMultifix0FailTest extends munit.FunSuite {
  def testDescription: String = "Non-linear recursion: zero usage of path in multifix"

  def expectedError: String = "Recursive definitions must be linear, e.g. recursive references must appear at least once in all the recursive definitions"

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
                val (pathResult, pathToAResult) = fix(pathBase, pathToABase)((path, pathToA) =>
                  val P = path.flatMap(p =>
                    tables.edges
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

class RecursiveConstraintLinear5Test extends SQLStringQueryTest[TCDB, Edge] {
  def testDescription: String = "Linear recursion: refs used more than once, but only once per definition, multifix"

  def query() =
    val pathBase = testDB.tables.edges
    val path2Base = testDB.tables.emptyEdges
//    type QT = (Query[Edge, ?], Query[Edge, ?])
//    type DT = (Tuple1[0], (0, 1))
//    type RQT = (RestrictedQuery[Edge, SetResult, Tuple1[0]], RestrictedQuery[Edge, SetResult, (0, 1)])
//    val (pathResult, path2Result) = fix[QT, DT, RQT](pathBase, path2Base)((path, path2) =>
    val (pathResult, path2Result) = fix(pathBase, path2Base)((path, path2) =>
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

class RecursiveConstraintInvalidFailTest extends munit.FunSuite {
  def testDescription: String = "Use multifix instead of fix for single definition"

  def expectedError: String = "Found:    (pathBase : tyql.Table[Edge])\nRequired: Tuple"

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
                  val (pathResult, path2Result) = fix(pathBase)((path, path2) =>
                    val P = path2.flatMap(p =>
                      path
                        .filter(e => p.q == e.x)
                        .map(e => (x = p.q, y = e.y).toRow)
                    ).distinct
                    val P2 = path2
                    (P, P2)
                  )
                  pathResult)
                """)
    assert(error.contains(expectedError), s"Expected substring '$expectedError' in '$error'")
  }
}

class RecursiveConstraintInvalid2FailTest extends munit.FunSuite {
  def testDescription: String = "Extra param in fix fns"

  def expectedError: String = "Wrong number of parameters"

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
                val (pathResult, path2Result) = fix(pathBase, path2Base)((path, path2, path3) =>
                  val P = path2.flatMap(p =>
                    path
                      .filter(e => p.q == e.x)
                      .map(e => (x = p.q, y = e.y).toRow)
                  ).distinct
                  val P2 = path2
                  (P, P2)
                )
                pathResult
                    )
                  """)
    assert(error.contains(expectedError), s"Expected substring '$expectedError' in '$error'")
  }
}

class RecursiveConstraintInvalid3FailTest extends munit.FunSuite {
  def testDescription: String = "Too few param in fix fns"

  def expectedError: String = "Wrong number of parameters"

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
                  val (pathResult, path2Result) = fix(pathBase, path2Base, path2Base)((path, path2) =>
                    val P = path2.flatMap(p =>
                      path
                        .filter(e => p.q == e.x)
                        .map(e => (x = p.q, y = e.y).toRow)
                    ).distinct
                    val P2 = path2
                    (P, P2)
                  )
                  pathResult
                      )
                    """)
    assert(error.contains(expectedError), s"Expected substring '$expectedError' in '$error'")
  }
}

class RecursiveConstraintInvalid4FailTest extends munit.FunSuite {
  def testDescription: String = "Extra recursive relation returned from fns"

  def expectedError: String = "Number of base cases must match the number of recursive definitions returned by fns"

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
      val P = path2.flatMap(p =>
        path
          .filter(e => p.q == e.x)
          .map(e => (x = p.q, y = e.y).toRow)
      ).distinct
      val P2 = path2
      (P, P2, P2)
    )
    pathResult
    )
    """)
    assert(error.contains(expectedError), s"Expected substring '$expectedError' in '$error'")
  }
}

class RecursiveConstraintInvalid5FailTest extends munit.FunSuite {
  def testDescription: String = "Too few recursive relation returned from fns"

  def expectedError: String = "Number of base cases must match the number of recursive definitions returned by fns"

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
      val P = path2.flatMap(p =>
        path
          .filter(e => p.q == e.x)
          .map(e => (x = p.q, y = e.y).toRow)
      ).distinct
      val P2 = path2
      Tuple1(P)
    )
    pathResult
    )
    """)
    assert(error.contains(expectedError), s"Expected substring '$expectedError' in '$error'")
  }
}

class RecursiveConstraintGroupbyInlineFailTest extends munit.FunSuite {
  def testDescription: String = "Try to call groupBy on join between recur + non-recur"

  def expectedError: String = "value groupBy is not a member of tyql.RestrictedQuery["

  test(testDescription) {
    val error: String =
      compileErrors(
        """
     // BOILERPLATE
     import language.experimental.namedTuples
     import tyql.{Table, Expr}
     import tyql.Expr.min

     type Edge = (x: Int, y: Int)

     val tables = (
       edges = Table[Edge]("edges"),
       edges2 = Table[Edge]("otherEdges"),
       emptyEdges = Table[Edge]("empty")
     )

    // TEST
    val edges = tables.edges.groupBy(e => (x = e.x).toRow, e => (x = e.x, y = min(e.y)).toRow)

    edges.fix(minReach =>
      minReach.flatMap(mr =>
        edges
          .filter(e => mr.y == e.x)
          .map(e => (x = mr.x, y = e.y).toRow)
      ).groupBy(
        row => (x = row.x).toRow,
        row => (x = row.x, min_y = min(row.y)).toRow
      )
    )
    """)
    assert(error.contains(expectedError), s"Expected substring '$expectedError' in '$error'")
  }
}

class RecursiveConstraintGroupbyMultifixFailTest extends munit.FunSuite {
  def testDescription: String = "Try to call groupBy on join between recur + non-recur, multifix"

  def expectedError: String = "value groupBy is not a member of tyql.RestrictedQuery["

  test(testDescription) {
    val error: String =
      compileErrors(
        """
     // BOILERPLATE
     import language.experimental.namedTuples
     import tyql.{Table, Expr}
     import tyql.Expr.min

     type Edge = (x: Int, y: Int)

     val tables = (
       edges = Table[Edge]("edges"),
       edges2 = Table[Edge]("otherEdges"),
       emptyEdges = Table[Edge]("empty")
     )

    // TEST
    val parentChild = tables.edges

    val ancestorBase = parentChild
    val descendantBase = parentChild

    val (ancestorResult, descendantResult) = fix(ancestorBase, descendantBase) { (ancestor, descendant) =>
      val newAncestor = ancestor.flatMap(a =>
        parentChild
          .filter(p => a.y == p.x)
          .map(p => (x = a.x, y = p.y).toRow)
      ).groupBy(x => (x = x.x).toRow, x => (x = x.x, y = min(x.y)))

      val newDescendant = descendant.flatMap(d =>
        parentChild
          .filter(p => d.x == p.y)
          .map(p => (x = d.y, y = p.x).toRow)
      )
      (newAncestor.distinct, newDescendant.distinct)
    }

    ancestorResult
    """)
    assert(error.contains(expectedError), s"Expected substring '$expectedError' in '$error'")
  }
}

class RecursiveConstraintNonlinearFailTest extends munit.FunSuite {
  def testDescription: String = "Use all args in one relation, none in the other, but with groupBy in one"

  def expectedError: String = "Recursive definitions must be linear:"

  test(testDescription) {
    val error: String =
      compileErrors(
        """
     // BOILERPLATE
     import language.experimental.namedTuples
     import tyql.{Table, Expr}
     import tyql.Expr.min

     type Edge = (x: Int, y: Int)

     val tables = (
       edges = Table[Edge]("edges"),
       edges2 = Table[Edge]("otherEdges"),
       emptyEdges = Table[Edge]("empty")
     )

    // TEST
    val parentChild = tables.edges

    val ancestorBase = parentChild
    val descendantBase = parentChild

    val (ancestorResult, descendantResult) = fix(ancestorBase, descendantBase) { (ancestor, descendant) =>
      val newAncestor = parentChild.groupBy(
        row => (x = row.x).toRow,
        row => (x = row.x, mutual_friend_count = count(row.y)).toRow
      )
      val newDescendant = descendant.flatMap(d =>
       ancestor
          .filter(p => d.x == p.y)
          .map(p => (x = d.y, y = p.x).toRow)
      )
      (newAncestor.distinct, newDescendant.distinct)
    }

    ancestorResult
    """)
    assert(error.contains(expectedError), s"Expected substring '$expectedError' in '$error'")
  }
}
class RecursiveConstraintAggregationMutualRecursionFailTest extends munit.FunSuite {
  def testDescription: String = "Aggregation in mutual recursion"

  def expectedError: String = "Recursive definitions must be linear:"

  test(testDescription) {
    val error: String =
      compileErrors(
        """
     // BOILERPLATE
     import language.experimental.namedTuples
     import tyql.{Table, Expr}
     import tyql.Expr.sum

    type Shares = (by: String, of: String, percent: Int)
    type Control = (com1: String, com2: String)
    type CompanyControlDB = (shares: Shares, control: Control)

    val tables = (
        shares = Table[Shares]("shares"),
        control = Table[Control]("control")
    )

    // TEST
    val (cshares, control) = fix(tables.shares, tables.control)((cshares, control) =>
      val csharesRecur = control.flatMap(con =>
        cshares
          .filter(cs => cs.by == con.com2)
          .map(cs => (by = con.com1, of = cs.of, percent = cs.percent))
      ).union(cshares)
        .groupBy(
          c => (by = c.by, of = c.of).toRow,
          c => (by = c.by, of = c.of, percent = sum(c.percent)).toRow
        ).distinct
      val controlRecur = cshares
        .filter(s => s.percent > 50)
        .map(s => (com1 = s.by, com2 = s.of))
        .distinct
      (csharesRecur, controlRecur)
    )
    control
    """)
    assert(error.contains(expectedError), s"Expected substring '$expectedError' in '$error'")
  }
}

class RecursionConstraintCategoryUnionAll2FailTest extends munit.FunSuite {
  def testDescription: String = "recursive query defined over bag, using unionAll, will not terminate!"
  def expectedError: String = "Found:    tyql.RestrictedQuery[(x : Int, y : Int), tyql.BagResult, Tuple1[(0 : Int)]]\nRequired: tyql.RestrictedQuery[Edge, tyql.SetResult, Tuple1[(0 : Int)]]"

  test(testDescription) {
    val error: String =
      compileErrors(
        """
           // BOILERPLATE
           import language.experimental.namedTuples
           import tyql.{Table, Expr, Query}

           type Edge = (x: Int, y: Int)
           type TCDB = (edges: Edge)

           val tables = (
             edges = Table[Edge]("edges")
           )

          // TEST
          val base = tables.edges
          base.fix(path =>
            path.flatMap(p =>
              tables.edges
                .filter(e => p.y == e.x)
                .map(e => (x = p.x, y = e.y).toRow)
            )// Removing 'distinct' will cause it to never terminate
          )
          """)
    assert(error.contains(expectedError), s"Expected substring '$expectedError' in '$error'")
  }
}

class RecursionFibFailTest extends munit.FunSuite {
  def testDescription: String = "Fibonacci example fails due to unsupported arithmetic on tyql.Expr"

  def expectedError: String =
    "value + is not a member of tyql.Expr[Int, tyql.NonScalarExpr, tyql.Restricted]"

  test(testDescription) {
    val error: String =
      compileErrors(
        """
           // BOILERPLATE
           import language.experimental.namedTuples
           import tyql.{Table, Expr, Query}
           import Query.fix

           type FibNum = (recursionDepth: Int, fibonacciNumber: Int, nextNumber: Int)
           val base = Table[FibNum]("base")

           base.fix(fib =>
             fib
               .filter(f => (f.recursionDepth + 1) < 10)
               .map(f => (recursionDepth = f.recursionDepth + 1, fibonacciNumber = f.nextNumber, nextNumber = f.fibonacciNumber + f.nextNumber).toRow)
               .distinct
           )
        """)
    assert(error.contains(expectedError), s"Expected substring '$expectedError' in '$error'")
  }
}

class RecursionShortestPathFailTest extends munit.FunSuite {
  def testDescription: String = "Shortest path query fails due to unsupported append operation on tyql.Expr"

  def expectedError: String =
    "value append is not a member of tyql.Expr[List[Int], tyql.NonScalarExpr, tyql.Restricted]"

  test(testDescription) {
    val error: String =
      compileErrors(
        """
           // BOILERPLATE
           import language.experimental.namedTuples
           import tyql.{Table, Expr, Query}
           import Query.fix

           type Edge = (x: Int, y: Int)
           type Path = (startNode: Int, endNode: Int, path: List[Int])
           val edge = Table[Edge]("edge")

           val pathBase = edge
             .map(e => (startNode = e.x, endNode = e.y, path = List(e.x, e.y).toExpr).toRow)
             .filter(p => p.startNode == 1)

           pathBase.fix(path =>
             path.flatMap(p =>
               edge
                 .filter(e => e.x == p.endNode)
                 .map(e => (startNode = p.startNode, endNode = e.y, path = p.path.append(e.y)).toRow)
             ).distinct
           )
        """)
    assert(error.contains(expectedError), s"Expected substring '$expectedError' in '$error'")
  }
}

class RecursionTreeFailTest extends munit.FunSuite {
  def testDescription: String = "Tag tree example fails due to unsupported prepend operation on tyql.Expr"

  def expectedError: String =
    "value prepend is not a member of tyql.Expr[List[String], tyql.NonScalarExpr, tyql.Restricted]"

  test(testDescription) {
    val error: String =
      compileErrors(
        """
           // BOILERPLATE
           import language.experimental.namedTuples
           import tyql.{Table, Expr, Query}
           import Query.fix

           type Tag = (id: Int, name: String, subclassof: Int)
           type TagHierarchy = (id: Int, source: String, path: List[String])
           val tag = Table[Tag]("tag")

           val tagHierarchy0 = tag
             .filter(t => t.subclassof == -1)
             .map(t => (id = t.id, source = t.name, path = List(t.name).toExpr).toRow)

           tagHierarchy0.fix(tagHierarchy1 =>
             tagHierarchy1.flatMap(hier =>
               tag
                 .filter(t => t.subclassof == hier.id)
                 .map(t => (id = t.id, source = t.name, path = hier.path.prepend(t.name)).toRow)
             ).distinct
           )
        """)
    assert(error.contains(expectedError), s"Expected substring '$expectedError' in '$error'")
  }
}

class AncestryFailTest extends munit.FunSuite {
  def testDescription: String = "Ancestry query fails due to unsupported arithmetic on tyql.Expr"

  def expectedError: String =
    "value + is not a member of tyql.Expr[Int, tyql.NonScalarExpr, tyql.Restricted]"

  test(testDescription) {
    val error: String =
      compileErrors(
        """
           // BOILERPLATE
           import language.experimental.namedTuples
           import tyql.{Table, Expr, Query}
           import Query.fix

           type Parent = (parent: String, child: String)
           val parent = Table[Parent]("parents")

           val base = parent.filter(p => p.parent == "Alice").map(e => (name = e.child, gen = Expr.IntLit(1)).toRow)

           base.fix(gen =>
             parent.flatMap(parent =>
               gen
                 .filter(g => parent.parent == g.name)
                 .map(g => (name = parent.child, gen = g.gen + 1).toRow)
             ).distinct
           )
        """)
    assert(error.contains(expectedError), s"Expected substring '$expectedError' in '$error'")
  }
}

class EvenOddFailTest extends munit.FunSuite {
  def testDescription: String = "Even-odd recursion fails due to unsupported arithmetic on tyql.Expr"

  def expectedError: String =
    "value + is not a member of tyql.Expr[Int, tyql.NonScalarExpr, tyql.Restricted]"

  test(testDescription) {
    val error: String =
      compileErrors(
        """
           // BOILERPLATE
           import language.experimental.namedTuples
           import tyql.{Table, Expr, Query}
           import Query.fix

           type Number = (id: Int, value: Int)
           val numbers = Table[Number]("numbers")

           val evenBase = numbers.filter(n => n.value == 0).map(n => (value = n.value, typ = Expr.StringLit("even")).toRow)
           val oddBase = numbers.filter(n => n.value == 1).map(n => (value = n.value, typ = Expr.StringLit("odd")).toRow)

           val (even, odd) = fix((evenBase, oddBase))((even, odd) =>
             val evenResult = numbers.flatMap(num =>
               odd.filter(o => num.value == o.value + 1).map(o => (value = num.value, typ = Expr.StringLit("even")))
             ).distinct
             val oddResult = numbers.flatMap(num =>
               even.filter(e => num.value == e.value + 1).map(e => (value = num.value, typ = Expr.StringLit("odd")))
             ).distinct
             (evenResult, oddResult)
           )
        """)
    assert(error.contains(expectedError), s"Expected substring '$expectedError' in '$error'")
  }
}

class RecursionSSSPFailTest extends munit.FunSuite {
  def testDescription: String = "Single source shortest path fails due to unsupported arithmetic on tyql.Expr"

  def expectedError: String =
    "value + is not a member of tyql.Expr[Int, tyql.NonScalarExpr, tyql.Restricted]"

  test(testDescription) {
    val error: String =
      compileErrors(
        """
           // BOILERPLATE
           import language.experimental.namedTuples
           import tyql.{Table, Expr, Query}
           import Query.fix

           type WeightedEdge = (src: Int, dst: Int, cost: Int)
           val edge = Table[WeightedEdge]("edge")

           val base = Table[(dst: Int, cost: Int)]("base")

           base.fix(sp =>
             edge.flatMap(edge =>
               sp
                 .filter(s => s.dst == edge.src)
                 .map(s => (dst = edge.dst, cost = s.cost + edge.cost).toRow)
             ).distinct
           )
        """)
    assert(error.contains(expectedError), s"Expected substring '$expectedError' in '$error'")
  }
}



//class TESTTEST extends SQLStringQueryTest[TCDB, Edge] {
//  def testDescription: String = "Live tests"
//
//  def query() =
//    val path = testDB.tables.edges
//    path.fix(path =>
//      path.flatMap(p =>
//        path
//          .filter(e => p.y == e.x)
//          .map(e => (x = p.x, y = e.y).toRow)
//      ).distinct
//    )
//
//  def expectedQueryPattern: String =
//    """
//        """
//}