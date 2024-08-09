package test.query.recursivev2

import test.{SQLStringQueryTest, TestDatabase}
import tyql.*
import tyql.Query.*

import scala.NamedTuple.*
import scala.language.experimental.namedTuples
import scala.language.implicitConversions

type Edge = (x: Int, y: Int)
type Edge2 = (z: Int, q: Int)
type TCDB = (edges: Edge, otherEdges: Edge2, emptyEdges: Edge)

given TCDBs: TestDatabase[TCDB] with
  override def tables = (
    edges = Table[Edge]("edges"),
    otherEdges = Table[Edge2]("otherEdges"),
    emptyEdges = Table[Edge]("empty")
  )

class Recursion1Test extends SQLStringQueryTest[TCDB, Edge] {
  def testDescription: String = "TC"

  def query() =
    val path = testDB.tables.edges
    path.fixV2(path =>
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
      WHERE recursive$A.y = edges$C.x); SELECT * FROM recursive$A
      """
}
class Recursion2Test extends SQLStringQueryTest[TCDB, Edge] {
  def testDescription: String = "TC with multiple base cases"

  def query() =
    val path = testDB.tables.edges.unionAll(testDB.tables.edges)
    path.fixV2(path =>
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
      SELECT * FROM edges as edges$E
        UNION ALL
      SELECT recursive$A.x as x, edges$C.y as y
      FROM recursive$A, edges as edges$C
      WHERE recursive$A.y = edges$C.x); SELECT * FROM recursive$A
      """
}

class Recursion3Test extends SQLStringQueryTest[TCDB, Edge] {
  def testDescription: String = "TC with multiple recursive cases"

  def query() =
    val path = testDB.tables.edges.unionAll(testDB.tables.edges)
    val path2 = path.fixV2(path =>
      path.flatMap(p =>
        testDB.tables.edges
          .filter(e => p.y == e.x)
          .map(e => (x = p.x, y = e.y).toRow)
      )
    )

    path2.fixV2(path =>
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
        SELECT * FROM edges as edges$E
          UNION ALL
        SELECT recursive$A.x as x, edges$C.y as y
        FROM recursive$A, edges as edges$C
        WHERE recursive$A.y = edges$C.x
          UNION ALL
        SELECT recursive$A.x as x, edges$F.y as y
        FROM recursive$A, edges as edges$F
        WHERE recursive$A.y = edges$F.x);
      SELECT * FROM recursive$A
        """
}

class Recursion4Test extends SQLStringQueryTest[TCDB, Int] {
  def testDescription: String = "TC with project"

  def query() =
    val path = testDB.tables.edges
    path.fixV2(path =>
      path.flatMap(p =>
        testDB.tables.edges
          .filter(e => p.y == e.x)
          .map(e => (x = p.x, y = e.y).toRow)
      )
    ).map(p => p.x)

  def expectedQueryPattern: String =
    """
      WITH RECURSIVE recursive$A AS
        (SELECT * FROM edges as edges$B
          UNION ALL
        SELECT recursive$A.x as x, edges$C.y as y
        FROM recursive$A, edges as edges$C
        WHERE recursive$A.y = edges$C.x); SELECT recursive$A.x FROM recursive$A
        """
}

class Recursion5Test extends SQLStringQueryTest[TCDB, Edge] {
  def testDescription: String = "TC with filter"

  def query() =
    val path = testDB.tables.edges
    path.fixV2(path =>
      path.flatMap(p =>
        testDB.tables.edges
          .filter(e => p.y == e.x)
          .map(e => (x = p.x, y = e.y).toRow)
      )
    ).filter(p => p.x > 1)

  def expectedQueryPattern: String =
    """
        WITH RECURSIVE recursive$A AS
          (SELECT * FROM edges as edges$B
            UNION ALL
          SELECT recursive$A.x as x, edges$C.y as y
          FROM recursive$A, edges as edges$C
          WHERE recursive$A.y = edges$C.x); SELECT * FROM recursive$A WHERE recursive$A.x > 1
          """
}

class Recursion6Test extends SQLStringQueryTest[TCDB, Int] {
  def testDescription: String = "TC with filter + map"

  def query() =
    val path = testDB.tables.edges
    path.fixV2(path =>
      path.flatMap(p =>
        testDB.tables.edges
          .filter(e => p.y == e.x)
          .map(e => (x = p.x, y = e.y).toRow)
      )
    ).filter(p => p.x > 1).map(p => p.x)

  def expectedQueryPattern: String =
    """
          WITH RECURSIVE recursive$A AS
            (SELECT * FROM edges as edges$B
              UNION ALL
            SELECT recursive$A.x as x, edges$C.y as y
            FROM recursive$A, edges as edges$C
            WHERE recursive$A.y = edges$C.x); SELECT recursive$A.x FROM recursive$A WHERE recursive$A.x > 1
            """
}

class NotRecursiveCTETest extends SQLStringQueryTest[TCDB, Edge] {
  def testDescription: String = "No recursion"

  def query() =
    val path = testDB.tables.edges
    path.fixV2(path =>
      testDB.tables.edges
    )
  def expectedQueryPattern: String =
    """
    WITH RECURSIVE recursive$A AS
      (SELECT * FROM edges as edges$B
        UNION ALL
      SELECT * FROM edges as edges$E);
    SELECT * FROM recursive$A
      """
}

type Location = (p1: Int, p2: Int)
type CSPADB = (assign: Location, dereference: Location, empty: Location)

given CSPADBs: TestDatabase[CSPADB] with
  override def tables = (
    assign = Table[Location]("assign"),
    dereference = Table[Location]("dereference"),
    empty = Table[Location]("empty") // TODO: define singleton for empty table?
  )

class RecursiveTwoTest extends  SQLStringQueryTest[TCDB, Edge] {
  def testDescription: String = "define 2 recursive relations"

  def query() =
    val pathBase = testDB.tables.edges
    val pathToABase = testDB.tables.emptyEdges
    val (pathResult, pathToAResult) = fixV2Two(pathBase, pathToABase)((path, pathToA) =>
      val P = path.flatMap(p =>
        testDB.tables.edges
          .filter(e => p.y == e.x)
          .map(e => (x = p.x, y = e.y).toRow)
      )
      val PtoA = path.filter(e => e.x == "A")
      (P, PtoA)
    )

    pathToAResult
  def expectedQueryPattern: String =
    """
    WITH RECURSIVE recursive51 AS (SELECT * FROM edges as edges52 UNION ALL SELECT recursive51.x as x, edges54.y as y FROM recursive51, edges as edges54 WHERE recursive51.y = edges54.x), recursive52 AS (SELECT * FROM empty as empty58 UNION ALL SELECT * FROM recursive51 WHERE recursive51.x = "A"); (SELECT * FROM recursive52) as subquery61
    """
}
