package test.query.recursive
import test.{SQLStringAggregationTest, SQLStringQueryTest, TestDatabase}
import tyql.*
import Query.{fix, fixTwo, fixThree, fixUntupled}

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

class RecursiveSCCTest extends SQLStringQueryTest[TCDB, Edge2] {
  def testDescription: String = "Multi-relation recursion"

  def query() =
    val path1 = testDB.tables.edges
    val path2 = testDB.tables.otherEdges

    // Option 1: untupled
    val (fullPath1, fullPath2) = fixUntupled((p: Query[Edge], q: Query[Edge2]) =>
      (p, q)
    )(path1, path2)

    // Option 2: tupled
    val (fullPath1a, fullPath2a) = fix((t: (Query[Edge], Query[Edge2])) =>
      (t._1, t._2)
    )((path1, path2))

    // Option 3: static tuple length
    val (fullPath1b, fullPath2b) = fixTwo(path1, path2)((p, q) =>
      (p, q)
    )

    fullPath2
  def expectedQueryPattern: String =
    """
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

class RecursiveCSPATest extends SQLStringQueryTest[CSPADB, Location] {
  def testDescription: String = "CSPA, example mutual recursion"

  def query() =
    val assign = testDB.tables.assign
    val dereference = testDB.tables.dereference

    val memoryAliasBase =
      // MemoryAlias(x, x) :- Assign(_, x)
      assign.map(a => (p1 = a.p2, p2 = a.p2))
        .unionAll(
          // MemoryAlias(x, x) :- Assign(x, _)
          assign.map(a => (p1 = a.p1, p2 = a.p1))
        )

    val valueFlowBase =
      assign // ValueFlow(y, x) :- Assign(y, x)
        .unionAll(
          // ValueFlow(x, x) :- Assign(x, _)
          assign.map(a => (p1 = a.p1, p2 = a.p1))
        ).unionAll(
          // ValueFlow(x, x) :- Assign(_, x)
          assign.map(a => (p1 = a.p2, p2 = a.p2))
        )

    val (valueFlowFinal, valueAliasFinal, memoryAliasFinal) = fixThree(valueFlowBase, testDB.tables.empty, memoryAliasBase)(
      (valueFlow, valueAlias, memoryAlias) =>
        val VF =
          // ValueFlow(x, y) :- (Assign(x, z), MemoryAlias(z, y))
          assign.flatMap(a =>
            memoryAlias
              .filter(m => a.p2 == m.p1)
              .map(m => (p1 = a.p1, p2 = m.p2)
            )
          ).unionAll(
            // ValueFlow(x, y) :- (ValueFlow(x, z), ValueFlow(z, y))
            valueFlow.flatMap(vf1 =>
              valueFlow
                .filter(vf2 => vf1.p2 == vf2.p1)
                .map(vf2 => (p1 = vf1.p1, p2 = vf2.p2))
            )
          )
        val MA =
          // MemoryAlias(x, w) :- (Dereference(y, x), ValueAlias(y, z), Dereference(z, w))
          dereference.flatMap(d1 =>
            valueAlias.flatMap(va =>
              dereference
                .filter(d2 => d1.p1 == va.p1 && va.p2 == d2.p1)
                .map(d2 => (p1 = d1.p2, p2 = d2.p2))
              )
            )
        val VA =
          // ValueAlias(x, y) :- (ValueFlow(z, x), ValueFlow(z, y))
          valueFlow.flatMap(vf1 =>
            valueFlow
              .filter(vf2 => vf1.p1 == vf2.p1)
              .map(vf2 => (p1 = vf1.p2, p2 = vf2.p2))
          ).unionAll(
            // ValueAlias(x, y) :- (ValueFlow(z, x), MemoryAlias(z, w), ValueFlow(w, y))
            valueFlow.flatMap(vf1 =>
              memoryAlias.flatMap(m =>
                valueFlow
                  .filter(vf2 => vf1.p1 == m.p1 && vf2.p1 == m.p2)
                  .map(vf2 => (p1 = vf1.p2, p2 = vf2.p2))
              )
            )
          )
        (VF, MA, VA)
    )
    ???
  def expectedQueryPattern: String =
    """
    """
}

class RecursiveCSPAComprehensionTest extends SQLStringQueryTest[CSPADB, Location] {
  def testDescription: String = "CSPA, but with comprehensions to see if nicer"

  def query() =
    val assign = testDB.tables.assign
    val dereference = testDB.tables.dereference

    val memoryAliasBase =
      // MemoryAlias(x, x) :- Assign(_, x)
      assign.map(a => (p1 = a.p2, p2 = a.p2))
        .unionAll(
          // MemoryAlias(x, x) :- Assign(x, _)
          assign.map(a => (p1 = a.p1, p2 = a.p1))
        )

    val valueFlowBase =
      assign // ValueFlow(y, x) :- Assign(y, x)
        .unionAll(
          // ValueFlow(x, x) :- Assign(x, _)
          assign.map(a => (p1 = a.p1, p2 = a.p1))
        ).unionAll(
          // ValueFlow(x, x) :- Assign(_, x)
          assign.map(a => (p1 = a.p2, p2 = a.p2))
        )

    val (valueFlowFinal, valueAliasFinal, memoryAliasFinal) = fixThree(valueFlowBase, testDB.tables.empty, memoryAliasBase)(
      (valueFlow, valueAlias, memoryAlias) =>
        // ValueFlow(x, y) :- (Assign(x, z), MemoryAlias(z, y))
        val vfDef1 =
          for
            a <- assign
            m <- memoryAlias
            if a.p2 == m.p1
          yield (p1 = a.p1, p2 = m.p2)
        // ValueFlow(x, y) :- (ValueFlow(x, z), ValueFlow(z, y))
        val vfDef2 =
          for
            vf1 <- valueFlow
            vf2 <- valueFlow
            if vf1.p2 == vf2.p1
          yield (p1 = vf1.p1, p2 = vf2.p2)
        val VF = vfDef1.unionAll(vfDef2)

        // MemoryAlias(x, w) :- (Dereference(y, x), ValueAlias(y, z), Dereference(z, w))
        val MA =
          for
            d1 <- dereference
            va <- valueAlias
            d2 <- dereference
            if d1.p1 == va.p1 && va.p2 == d2.p1
          yield (p1 = d1.p2, p2 = d2.p2)

        // ValueAlias(x, y) :- (ValueFlow(z, x), ValueFlow(z, y))
        val vaDef1 =
          for
            vf1 <- valueFlow
            vf2 <- valueFlow
            if vf1.p1 == vf2.p1
          yield (p1 = vf1.p2, p2 = vf2.p2)
        // ValueAlias(x, y) :- (ValueFlow(z, x), MemoryAlias(z, w), ValueFlow(w, y))
        val vaDef2 =
          for
            vf1 <- valueFlow
            m <- memoryAlias
            vf2 <- valueFlow
            if vf1.p1 == m.p1 && vf2.p1 == m.p2
          yield (p1 = vf1.p2, p2 = vf2.p2)
        val VA = vaDef1.unionAll(vaDef2)

        (VF, MA, VA)
    )
    ???
  def expectedQueryPattern: String =
    """
    """
}