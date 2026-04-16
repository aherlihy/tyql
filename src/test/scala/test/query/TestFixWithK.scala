package test.query.fixwithk

import test.{SQLStringQueryTest, TestDatabase}
import tyql.*

import language.experimental.namedTuples
import NamedTuple.*
import scala.language.implicitConversions

type Edge = (x: Int, y: Int)
type TCDB = (edges: Edge)

given TCDBs: TestDatabase[TCDB] with
  override def tables = (
    edges = Table[Edge]("edges")
  )

// Positive SQL tests — verify κ-tagged fix produces identical SQL.

class FixWithK_TCTest extends SQLStringQueryTest[TCDB, Edge] {
  def testDescription: String = "TC via restrictedFix with κ"

  def query() =
    testDB.tables.edges.restrictedFix([K] => path =>
      path
        .flatMap(p => testDB.tables.edges.filter(e => p.y == e.x).map(e => (x = p.x, y = e.y).toRow))
        .distinct
    )

  def expectedQueryPattern: String =
    """
    WITH RECURSIVE recursive$A AS
      ((SELECT * FROM edges as edges$B)
        UNION
      ((SELECT ref$D.x as x, edges$C.y as y
      FROM recursive$A as ref$D, edges as edges$C
      WHERE ref$D.y = edges$C.x))) SELECT * FROM recursive$A as recref$E
      """
}

class FixWithK_TCFilteredTest extends SQLStringQueryTest[TCDB, Edge] {
  def testDescription: String = "TC + outer filter via restrictedFix with κ"

  def query() =
    testDB.tables.edges.restrictedFix([K] => path =>
      path
        .flatMap(p => testDB.tables.edges.filter(e => p.y == e.x).map(e => (x = p.x, y = e.y).toRow))
        .distinct
    ).filter(p => p.x > 1)

  def expectedQueryPattern: String =
    """
        WITH RECURSIVE recursive$A AS
          ((SELECT * FROM edges as edges$B)
            UNION
          ((SELECT ref$Z.x as x, edges$C.y as y
          FROM recursive$A as ref$Z, edges as edges$C
          WHERE ref$Z.y = edges$C.x))) SELECT * FROM recursive$A as recref$D WHERE recref$D.x > 1
          """
}

// Negative compile tests — verify κ + linearity are enforced.

class FixWithKCompileTest extends munit.FunSuite {

  test("κ violation: inner body captures the outer ref") {
    val error: String =
      compileErrors(
        """
           import language.experimental.namedTuples
           import tyql.*
           import scala.language.implicitConversions

           type Edge = (x: Int, y: Int)
           val edges: Table[Edge] = Table[Edge]("edges")

           edges.restrictedFix([K1] => outer =>
             edges.restrictedFix([K2] => inner =>
               outer
                 .flatMap(p => edges.filter(e => p.y == e.x).map(e => (x = p.x, y = e.y).toRow))
                 .distinct
             )
           )
        """)
    assert(error.nonEmpty, "expected a κ-check compile error")
  }

  test("linearity violation: body uses the ref twice") {
    val error: String =
      compileErrors(
        """
           import language.experimental.namedTuples
           import tyql.*
           import scala.language.implicitConversions

           type Edge = (x: Int, y: Int)
           val edges: Table[Edge] = Table[Edge]("edges")

           edges.restrictedFix([K] => path =>
             path.flatMap(p1 =>
               path.filter(e => p1.y == e.x).map(e => (x = p1.x, y = e.y).toRow)
             ).distinct
           )
        """)
    assert(error.nonEmpty, "expected linearity compile error")
  }
}
