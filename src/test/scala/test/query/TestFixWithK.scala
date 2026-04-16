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

// Positive SQL tests — verify FixWithK produces identical SQL to Query.restrictedFix.

class FixWithK_TCTest extends SQLStringQueryTest[TCDB, Edge] {
  def testDescription: String = "TC via FixWithK.restrictedFixPoly"

  def query() =
    FixWithK.restrictedFixPoly(Tuple1(testDB.tables.edges)) { [K] => refs =>
      val path = refs._1
      Tuple1(
        path
          .flatMap(p => testDB.tables.edges.filter(e => p.y == e.x).map(e => (x = p.x, y = e.y).toRow))
          .distinct
      )
    }._1

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
  def testDescription: String = "TC + outer filter via FixWithK.restrictedFixPoly"

  def query() =
    FixWithK.restrictedFixPoly(Tuple1(testDB.tables.edges)) { [K] => refs =>
      val path = refs._1
      Tuple1(
        path
          .flatMap(p => testDB.tables.edges.filter(e => p.y == e.x).map(e => (x = p.x, y = e.y).toRow))
          .distinct
      )
    }._1.filter(p => p.x > 1)

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

class FixWithK_TCProjectTest extends SQLStringQueryTest[TCDB, Int] {
  def testDescription: String = "TC + outer map via FixWithK.restrictedFixPoly"

  def query() =
    FixWithK.restrictedFixPoly(Tuple1(testDB.tables.edges)) { [K] => refs =>
      val path = refs._1
      Tuple1(
        path
          .flatMap(p => testDB.tables.edges.filter(e => p.y == e.x).map(e => (x = p.x, y = e.y).toRow))
          .distinct
      )
    }._1.map(p => p.x)

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

           FixWithK.restrictedFixPoly(Tuple1(edges)) { [K1] => outerRefs =>
             val outer = outerRefs._1
             FixWithK.restrictedFixPoly(Tuple1(edges)) { [K2] => innerRefs =>
               Tuple1(
                 outer
                   .flatMap(p => edges.filter(e => p.y == e.x).map(e => (x = p.x, y = e.y).toRow))
                   .distinct
               )
             }
             Tuple1(outer.distinct)
           }
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

           FixWithK.restrictedFixPoly(Tuple1(edges)) { [K] => refs =>
             val path = refs._1
             Tuple1(
               path.flatMap(p1 =>
                 path.filter(e => p1.y == e.x).map(e => (x = p1.x, y = e.y).toRow)
               ).distinct
             )
           }
        """)
    assert(error.nonEmpty, "expected linearity compile error")
  }
}
