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

// Instance restrictedFixK — κ-tagged via [K] =>.

class RestrictedFixK_TCTest extends SQLStringQueryTest[TCDB, Edge] {
  def testDescription: String = "TC via restrictedFixK"

  def query() =
    testDB.tables.edges.restrictedFixK([K] => path =>
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

class RestrictedFixK_TCFilteredTest extends SQLStringQueryTest[TCDB, Edge] {
  def testDescription: String = "TC + outer filter via restrictedFixK"

  def query() =
    testDB.tables.edges.restrictedFixK([K] => path =>
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

// Instance fixK with non-default options — κ-tagged via [K] =>.

class FixK_BagTCTest extends SQLStringQueryTest[TCDB, Edge] {
  def testDescription: String = "TC via fixK with bag option"

  def query() =
    testDB.tables.edges.fixK((constructorFreedom = RestrictedConstructors(), monotonicity = Monotone(), category = BagResult(), linearity = Linear(), mutual = NoMutual()))(
      [K] => pathRec =>
        pathRec.flatMap(p =>
          testDB.tables.edges
            .filter(e => p.y == e.x)
            .map(e => (x = e.y, y = e.y).toRow)
        ).unionAll(testDB.tables.edges)
    )

  def expectedQueryPattern: String =
    """
WITH RECURSIVE recursive$A AS ((SELECT * FROM edges as edges$B) UNION ALL ((SELECT edges$C.y as x, edges$C.y as y FROM recursive$A as ref$D, edges as edges$C WHERE ref$D.y = edges$C.x) UNION ALL (SELECT * FROM edges as edges$E))) SELECT * FROM recursive$A as recref$F    """
}

// Companion restrictedFix — κ-tagged via explicit K type param.

class CompanionRestrictedFix_TCTest extends SQLStringQueryTest[TCDB, Edge] {
  def testDescription: String = "TC via Query.restrictedFix with κ"

  def query() =
    val edges = testDB.tables.edges
    Query.restrictedFix[Any, Tuple1[Query[Edge, ?]], Tuple1[RestrictedQuery[Edge, SetResult, Tuple1[0 & Any], RestrictedConstructors, MonotoneRestriction]]](
      Tuple1(edges)
    )(refs =>
      val path = refs._1
      Tuple1(
        path
          .flatMap(p => edges.filter(e => p.y == e.x).map(e => (x = p.x, y = e.y).toRow))
          .distinct
      )
    )._1.asInstanceOf[Query[Edge, BagResult]]

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

// Negative compile tests.

class FixWithKCompileTest extends munit.FunSuite {

  test("κ violation: inner restrictedFixK body captures the outer ref") {
    val error: String =
      compileErrors(
        """
           import language.experimental.namedTuples
           import tyql.*
           import scala.language.implicitConversions

           type Edge = (x: Int, y: Int)
           val edges: Table[Edge] = Table[Edge]("edges")

           edges.restrictedFixK([K1] => outer =>
             edges.restrictedFixK([K2] => inner =>
               outer
                 .flatMap(p => edges.filter(e => p.y == e.x).map(e => (x = p.x, y = e.y).toRow))
                 .distinct
             )
           )
        """)
    assert(error.nonEmpty, "expected a κ-check compile error")
  }

  test("linearity violation: restrictedFixK body uses the ref twice") {
    val error: String =
      compileErrors(
        """
           import language.experimental.namedTuples
           import tyql.*
           import scala.language.implicitConversions

           type Edge = (x: Int, y: Int)
           val edges: Table[Edge] = Table[Edge]("edges")

           edges.restrictedFixK([K] => path =>
             path.flatMap(p1 =>
               path.filter(e => p1.y == e.x).map(e => (x = p1.x, y = e.y).toRow)
             ).distinct
           )
        """)
    assert(error.nonEmpty, "expected linearity compile error")
  }

  test("linearity violation: restrictedFix body uses the ref twice") {
    val error: String =
      compileErrors(
        """
           import language.experimental.namedTuples
           import tyql.*
           import scala.language.implicitConversions

           type Edge = (x: Int, y: Int)
           val edges: Table[Edge] = Table[Edge]("edges")

           edges.restrictedFix(path =>
             path.flatMap(p1 =>
               path.filter(e => p1.y == e.x).map(e => (x = p1.x, y = e.y).toRow)
             ).distinct
           )
        """)
    assert(error.nonEmpty, "expected linearity compile error")
  }
}
