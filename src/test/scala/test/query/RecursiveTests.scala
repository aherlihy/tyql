package test.query.recursive
import test.{SQLStringQueryTest, SQLStringAggregationTest, TestDatabase}

import tyql.*
import Query.{fix,fix2}
import language.experimental.namedTuples
import NamedTuple.*
import scala.language.implicitConversions

type Edge = (x: Int, y: Int)
type Edge2 = (z: Int, q: Int)
type TCDB = (edges: Edge, otherEdges: Edge2)

given TCDBs: TestDatabase[TCDB] with
  override def tables = (
    edges = Table[Edge]("edges"),
    otherEdges = Table[Edge2]("otherEdges")
  )

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
      WHERE recursive$A.y = edges$C.x); SELECT * FROM recursive$A
      """
}
class Recursion2Test extends SQLStringQueryTest[TCDB, Edge] {
  def testDescription: String = "TC with multiple base cases"

  def query() =
    val path = testDB.tables.edges.unionAll(testDB.tables.edges)
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
    val path2 = path.fix(path =>
      path.flatMap(p =>
        testDB.tables.edges
          .filter(e => p.y == e.x)
          .map(e => (x = p.x, y = e.y).toRow)
      )
    )

    path2.fix(path =>
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
    path.fix(path =>
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
    path.fix(path =>
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
    path.fix(path =>
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
    path.fix(path =>
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
//
//class RecursiveSCCTest extends SQLStringQueryTest[TCDB, Edge2] {
//  def testDescription: String = "Multi-relation recursion"
//
//  def query() =
//    val path1 = testDB.tables.edges//.map(p => p)
//    val path2 = testDB.tables.otherEdges//.map(p => p)
//
////    val (fullPath1, fullPath2) = fix((p: (Query[Edge], Query[Edge2])) =>
////      (p._1, p._2)
////    )((path1, path2))
//
//    val (fullPath1a, fullPath2a) = fix2((p: Query[Edge], q: Query[Edge2]) =>
//      (p, q)
//    )(path1, path2)
//
//    path1.fix(path =>
//      path2.fix(path2 =>
//
//      )
//    )
//
//    fullPath2
//  def expectedQueryPattern: String =
//    """
//    """
//}
