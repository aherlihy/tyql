package test.query.recursive
import test.{SQLStringAggregationTest, SQLStringQueryTest, TestDatabase}
import tyql.*
import Query.{fix, unrestrictedFix, unrestrictedFixImpl}
import Expr.{IntLit, count, max, min, sum}

import language.experimental.namedTuples
import NamedTuple.*
import scala.language.implicitConversions

type Edge = (x: Int, y: Int)
type Edge2 = (z: Int, q: Int)
type TCDB = (edges: Edge, otherEdges: Edge2, emptyEdges: Edge)

import tyql.Dialect.ansi.given

given TCDBs: TestDatabase[TCDB] with
  override def tables = (
    edges = Table[Edge]("edges"),
    otherEdges = Table[Edge2]("otherEdges"),
    emptyEdges = Table[Edge]("empty")
  )

  override def init(): String =
    """
      CREATE TABLE edges (
        x INT,
        y INT
      );
      CREATE TABLE emptyEdges (
        x INT,
        y INT
      );
     CREATE TABLE otherEdges (
        z INT,
        q INT
      );
    """

class Recursion1Test extends SQLStringQueryTest[TCDB, Edge] {
  def testDescription: String = "TC"

  def dbSetup: String = """
    CREATE TABLE edges (
      x INT,
      y INT
    );

    CREATE TABLE empty (
        x INT,
        y INT
    );

    INSERT INTO edges (x, y) VALUES (1, 2);
    INSERT INTO edges (x, y) VALUES (2, 3);
    INSERT INTO edges (x, y) VALUES (3, 4);
    INSERT INTO edges (x, y) VALUES (4, 5);
    INSERT INTO edges (x, y) VALUES (5, 6);
  """

  def query() =
    val path = testDB.tables.edges
    path.fix(path =>
      path.flatMap(p =>
        testDB.tables.edges
          .filter(e => p.y == e.x)
          .map(e => (x = p.x, y = e.y).toRow)
      ).distinct
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

class Recursion2Test extends SQLStringQueryTest[TCDB, Edge] {
  def testDescription: String = "TC with multiple base cases"

  def query() =
    val path = testDB.tables.edges.union(testDB.tables.edges)
    path.fix(path =>
      path.flatMap(p =>
        testDB.tables.edges
          .filter(e => p.y == e.x)
          .map(e => (x = p.x, y = e.y).toRow)
      ).distinct
    )
  def expectedQueryPattern: String =
    """
    WITH RECURSIVE recursive$A AS
      ((SELECT * FROM edges as edges$B)
        UNION
      ((SELECT * FROM edges as edges$E)
        UNION
      (SELECT ref$D.x as x, edges$C.y as y
      FROM recursive$A as ref$D, edges as edges$C
      WHERE ref$D.y = edges$C.x)))
    SELECT * FROM recursive$A as recref$F
      """
}

// TODO: decide semantics, subquery, or flatten?
//class Recursion3Test extends SQLStringQueryTest[TCDB, Edge] {
//  def testDescription: String = "TC with multiple recursive cases"
//
//  def query() =
//    val path = testDB.tables.edges.union(testDB.tables.edges)
//    val path2 = path.fix(path =>
//      path.flatMap(p =>
//        testDB.tables.edges
//          .filter(e => p.y == e.x)
//          .map(e => (x = p.x, y = e.y).toRow)
//      )
//    )
//
//    path2.fix(path =>
//      path.flatMap(p =>
//        testDB.tables.edges
//          .filter(e => p.y == e.x)
//          .map(e => (x = p.x, y = e.y).toRow)
//      )
//    )
//
//  def expectedQueryPattern: String =
//    """
//      WITH RECURSIVE recursive$A AS
//        (SELECT * FROM edges as edges$B
//          UNION
//        SELECT * FROM edges as edges$E
//          UNION
//        SELECT recursive$A.x as x, edges$C.y as y
//        FROM recursive$A, edges as edges$C
//        WHERE recursive$A.y = edges$C.x
//          UNION
//        SELECT recursive$A.x as x, edges$F.y as y
//        FROM recursive$A, edges as edges$F
//        WHERE recursive$A.y = edges$F.x);
//      SELECT * FROM recursive$A
//        """
//}

class Recursion4Test extends SQLStringQueryTest[TCDB, Int] {
  def testDescription: String = "TC with project"

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

class Recursion5Test extends SQLStringQueryTest[TCDB, Edge] {
  def testDescription: String = "TC with filter"

  def query() =
    val path = testDB.tables.edges
    path.fix(path =>
      path.flatMap(p =>
        testDB.tables.edges
          .filter(e => p.y == e.x)
          .map(e => (x = p.x, y = e.y).toRow)
      ).distinct
    ).filter(p => p.x > 1)

  def expectedQueryPattern: String =
    """
        WITH RECURSIVE recursive$A AS
          ((SELECT * FROM edges as edges$B)
            UNION
          ((SELECT ref$Z.x as x, edges$C.y as y
          FROM recursive$A as ref$Z, edges as edges$C
          WHERE ref$Z.y = edges$C.x))) SELECT * FROM recursive$A as recref$X WHERE recref$X.x > 1
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
      ).distinct
    ).filter(p => p.x > 1).map(p => p.x)

  def expectedQueryPattern: String =
    """
          WITH RECURSIVE recursive$A AS
            ((SELECT * FROM edges as edges$B)
              UNION
            ((SELECT ref$Z.x as x, edges$C.y as y
            FROM recursive$A as ref$Z, edges as edges$C
            WHERE ref$Z.y = edges$C.x))) SELECT recref$X.x FROM recursive$A as recref$X WHERE recref$X.x > 1
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

  override def init(): String =
    """
  CREATE TABLE assign (
      p1 INT,
      p2 INT
    );
    CREATE TABLE dereference (
      p1 INT,
      p2 INT
    );
    CREATE TABLE empty (
      p1 INT,
      p2 INT
    );
  """

class RecursiveTwoMultiTest extends SQLStringQueryTest[TCDB, Edge] {
  def testDescription: String = "define 2 recursive relations, use multifix"

  def query() =
    val pathBase = testDB.tables.edges
    val pathToABase = testDB.tables.emptyEdges

    val (pathResult, pathToAResult) = fix(pathBase, pathToABase)((path, pathToA) =>
      val P = path.flatMap(p =>
        testDB.tables.edges
          .filter(e => p.y == e.x)
          .map(e => (x = p.x, y = e.y).toRow)
      )
      val PtoA = pathToA.filter(e => e.x == 1)
      (P.distinct, PtoA.distinct)
    )

    pathToAResult

  def expectedQueryPattern: String =
    """
      WITH RECURSIVE
          recursive$P AS
            ((SELECT * FROM edges as edges$F)
                UNION
             ((SELECT ref$Z.x as x, edges$C.y as y
             FROM recursive$P as ref$Z, edges as edges$C
             WHERE ref$Z.y = edges$C.x))),
          recursive$A AS
           ((SELECT * FROM empty as empty$D)
              UNION
            ((SELECT * FROM recursive$A as ref$X WHERE ref$X.x = 1)))
      SELECT * FROM recursive$A as recref$Q
      """
}

// This should fail because non-linear

class RecursiveSelfJoinTest extends SQLStringQueryTest[TCDB, Edge] {
  def testDescription: String = "define 2 recursive relations with one self join"

  def query() =
    val pathBase = testDB.tables.edges
    val pathToABase = testDB.tables.emptyEdges

    val (pathResult, pathToAResult) = unrestrictedFix(pathBase, pathToABase)((path, pathToA) =>
      val P = path.flatMap(p =>
        path
          .filter(p2 => p.y == p2.x)
          .map(p2 => (x = p.x, y = p2.y).toRow)
      )
      val PtoA = pathToA.filter(e => e.x == 9)
      (P.distinct, PtoA.distinct)
    )

    pathToAResult

  def expectedQueryPattern: String =
    """
        WITH RECURSIVE
            recursive$P AS
              ((SELECT * FROM edges as edges$F)
                  UNION
               ((SELECT ref$Z.x as x, ref$Y.y as y
               FROM recursive$P as ref$Z, recursive$P as ref$Y
               WHERE ref$Z.y = ref$Y.x))),
            recursive$A AS
             ((SELECT * FROM empty as empty$D)
                UNION
              ((SELECT * FROM recursive$A as ref$Q WHERE ref$Q.x = 9)))
        SELECT * FROM recursive$A as recref$S
        """
}

class RecursiveSelfJoin2Test extends SQLStringQueryTest[TCDB, Edge] {
  def testDescription: String = "define 2 recursive relations with one self join, multiple fitler"

  def query() =
    val pathBase = testDB.tables.edges
    val pathToABase = testDB.tables.emptyEdges

    val (pathResult, pathToAResult) = unrestrictedFix(pathBase, pathToABase)((path, pathToA) =>
      val P = path.flatMap(p =>
        path
          .filter(p2 => p.y == p2.x)
          .filter(p2 => p.x != p2.y) // ignore it makes no sense
          .map(p2 => (x = p.x, y = p2.y).toRow)
      )
      val PtoA = path.filter(e => e.x == 8)
      (P.distinct, PtoA.distinct)
    )

    pathToAResult

  def expectedQueryPattern: String =
    """
        WITH RECURSIVE
            recursive$P AS
              ((SELECT * FROM edges as edges$F)
                  UNION
               ((SELECT ref$Z.x as x, ref$Y.y as y
               FROM recursive$P as ref$Z, recursive$P as ref$Y
               WHERE ref$Z.x <> ref$Y.y AND ref$Z.y = ref$Y.x))),
            recursive$A AS
             ((SELECT * FROM empty as empty$D)
                UNION
              ((SELECT * FROM recursive$P as ref$Q WHERE ref$Q.x = 8)))
        SELECT * FROM recursive$A as recref$S
        """
}

class RecursiveCSPADistinctTest extends SQLStringQueryTest[CSPADB, Location] {
  def testDescription: String = "CSPA, example mutual recursion"

  def query() =
    val assign = testDB.tables.assign
    val dereference = testDB.tables.dereference

    val memoryAliasBase =
      // MemoryAlias(x, x) :- Assign(_, x)
      assign.map(a => (p1 = a.p2, p2 = a.p2).toRow)
        .unionAll(
          // MemoryAlias(x, x) :- Assign(x, _)
          assign.map(a => (p1 = a.p1, p2 = a.p1).toRow)
        ).distinct

    val valueFlowBase =
      assign // ValueFlow(y, x) :- Assign(y, x)
        .unionAll(
          // ValueFlow(x, x) :- Assign(x, _)
          assign.map(a => (p1 = a.p1, p2 = a.p1).toRow)
        ).unionAll(
          // ValueFlow(x, x) :- Assign(_, x)
          assign.map(a => (p1 = a.p2, p2 = a.p2).toRow)
        ).distinct

    val (valueFlowFinal, valueAliasFinal, memoryAliasFinal) = unrestrictedFix(
      valueFlowBase,
      testDB.tables.empty,
      memoryAliasBase
    )((valueFlow, valueAlias, memoryAlias) =>
      val VF =
        // ValueFlow(x, y) :- (Assign(x, z), MemoryAlias(z, y))
        assign.flatMap(a =>
          memoryAlias
            .filter(m => a.p2 == m.p1)
            .map(m => (p1 = a.p1, p2 = m.p2).toRow)
        ).unionAll(
          // ValueFlow(x, y) :- (ValueFlow(x, z), ValueFlow(z, y))
          valueFlow.flatMap(vf1 =>
            valueFlow
              .filter(vf2 => vf1.p2 == vf2.p1)
              .map(vf2 => (p1 = vf1.p1, p2 = vf2.p2).toRow)
          )
        ).distinct
      val MA =
        // MemoryAlias(x, w) :- (Dereference(y, x), ValueAlias(y, z), Dereference(z, w))
        dereference.flatMap(d1 =>
          valueAlias.flatMap(va =>
            dereference
              .filter(d2 => d1.p1 == va.p1 && va.p2 == d2.p1)
              .map(d2 => (p1 = d1.p2, p2 = d2.p2).toRow)
          )
        ).distinct

      val VA =
        // ValueAlias(x, y) :- (ValueFlow(z, x), ValueFlow(z, y))
        valueFlow.flatMap(vf1 =>
          valueFlow
            .filter(vf2 => vf1.p1 == vf2.p1)
            .map(vf2 => (p1 = vf1.p2, p2 = vf2.p2).toRow)
        ).unionAll(
          // ValueAlias(x, y) :- (ValueFlow(z, x), MemoryAlias(z, w), ValueFlow(w, y))
          valueFlow.flatMap(vf1 =>
            memoryAlias.flatMap(m =>
              valueFlow
                .filter(vf2 => vf1.p1 == m.p1 && vf2.p1 == m.p2)
                .map(vf2 => (p1 = vf1.p2, p2 = vf2.p2).toRow)
            )
          )
        ).distinct
      (VF, MA, VA)
    )
    valueFlowFinal

  def expectedQueryPattern: String =
    """
    WITH RECURSIVE
      recursive$A AS
        (((SELECT * FROM assign as assign$D)
          UNION ALL
        (SELECT assign$E.p1 as p1, assign$E.p1 as p2 FROM assign as assign$E)
          UNION ALL
        (SELECT assign$F.p2 as p1, assign$F.p2 as p2 FROM assign as assign$F))
          UNION
        (((SELECT assign$G.p1 as p1, ref$J.p2 as p2
        FROM assign as assign$G, recursive$C as ref$J
        WHERE assign$G.p2 = ref$J.p1)
          UNION ALL
        (SELECT ref$K.p1 as p1, ref$L.p2 as p2
        FROM recursive$A as ref$K, recursive$A as ref$L
        WHERE ref$K.p2 = ref$L.p1)))),
      recursive$B AS
        ((SELECT * FROM empty as empty$M)
          UNION
        ((SELECT dereference$N.p2 as p1, dereference$O.p2 as p2
        FROM dereference as dereference$N, recursive$B as ref$P, dereference as dereference$O
        WHERE dereference$N.p1 = ref$P.p1 AND ref$P.p2 = dereference$O.p1))),
      recursive$C AS
        (((SELECT assign$H.p2 as p1, assign$H.p2 as p2 FROM assign as assign$H)
          UNION ALL
        (SELECT assign$I.p1 as p1, assign$I.p1 as p2 FROM assign as assign$I))
          UNION
        (((SELECT ref$Q.p2 as p1, ref$R.p2 as p2
        FROM recursive$A as ref$Q, recursive$A as ref$R
        WHERE ref$Q.p1 = ref$R.p1)
          UNION ALL
        (SELECT ref$S.p2 as p1, ref$T.p2 as p2
        FROM recursive$A as ref$S, recursive$C as ref$U, recursive$A as ref$T
        WHERE ref$S.p1 = ref$U.p1 AND ref$T.p1 = ref$U.p2))))
    SELECT * FROM recursive$A as recref$V
    """
}

class RecursiveCSPATest extends SQLStringQueryTest[CSPADB, Location] {
  def testDescription: String = "CSPA, example mutual recursion"

  def query() =
    val assign = testDB.tables.assign
    val dereference = testDB.tables.dereference

    val memoryAliasBase =
      // MemoryAlias(x, x) :- Assign(_, x)
      assign.map(a => (p1 = a.p2, p2 = a.p2).toRow)
        .union(
          // MemoryAlias(x, x) :- Assign(x, _)
          assign.map(a => (p1 = a.p1, p2 = a.p1).toRow)
        )

    val valueFlowBase =
      assign // ValueFlow(y, x) :- Assign(y, x)
        .union(
          // ValueFlow(x, x) :- Assign(x, _)
          assign.map(a => (p1 = a.p1, p2 = a.p1).toRow)
        ).union(
          // ValueFlow(x, x) :- Assign(_, x)
          assign.map(a => (p1 = a.p2, p2 = a.p2).toRow)
        )

    val (valueFlowFinal, valueAliasFinal, memoryAliasFinal) = unrestrictedFix(
      valueFlowBase,
      testDB.tables.empty,
      memoryAliasBase
    )((valueFlow, valueAlias, memoryAlias) =>
      val VF =
        // ValueFlow(x, y) :- (Assign(x, z), MemoryAlias(z, y))
        assign.flatMap(a =>
          memoryAlias
            .filter(m => a.p2 == m.p1)
            .map(m => (p1 = a.p1, p2 = m.p2).toRow)
        ).union(
          // ValueFlow(x, y) :- (ValueFlow(x, z), ValueFlow(z, y))
          valueFlow.flatMap(vf1 =>
            valueFlow
              .filter(vf2 => vf1.p2 == vf2.p1)
              .map(vf2 => (p1 = vf1.p1, p2 = vf2.p2).toRow)
          )
        )
      val MA =
        // MemoryAlias(x, w) :- (Dereference(y, x), ValueAlias(y, z), Dereference(z, w))
        dereference.flatMap(d1 =>
          valueAlias.flatMap(va =>
            dereference
              .filter(d2 => d1.p1 == va.p1 && va.p2 == d2.p1)
              .map(d2 => (p1 = d1.p2, p2 = d2.p2).toRow)
          )
        ).distinct

      val VA =
        // ValueAlias(x, y) :- (ValueFlow(z, x), ValueFlow(z, y))
        valueFlow.flatMap(vf1 =>
          valueFlow
            .filter(vf2 => vf1.p1 == vf2.p1)
            .map(vf2 => (p1 = vf1.p2, p2 = vf2.p2).toRow)
        ).union(
          // ValueAlias(x, y) :- (ValueFlow(z, x), MemoryAlias(z, w), ValueFlow(w, y))
          valueFlow.flatMap(vf1 =>
            memoryAlias.flatMap(m =>
              valueFlow
                .filter(vf2 => vf1.p1 == m.p1 && vf2.p1 == m.p2)
                .map(vf2 => (p1 = vf1.p2, p2 = vf2.p2).toRow)
            )
          )
        )
      (VF, MA, VA)
    )
    valueFlowFinal

  def expectedQueryPattern: String =
    """
    WITH RECURSIVE
      recursive$A AS
        ((SELECT * FROM assign as assign$D)
          UNION
        ((SELECT assign$E.p1 as p1, assign$E.p1 as p2 FROM assign as assign$E)
          UNION
        (SELECT assign$F.p2 as p1, assign$F.p2 as p2 FROM assign as assign$F)
          UNION
        (SELECT assign$G.p1 as p1, ref$J.p2 as p2
        FROM assign as assign$G, recursive$C as ref$J
        WHERE assign$G.p2 = ref$J.p1)
          UNION
        (SELECT ref$K.p1 as p1, ref$L.p2 as p2
        FROM recursive$A as ref$K, recursive$A as ref$L
        WHERE ref$K.p2 = ref$L.p1))),
      recursive$B AS
        ((SELECT * FROM empty as empty$M)
          UNION
        ((SELECT dereference$N.p2 as p1, dereference$O.p2 as p2
        FROM dereference as dereference$N, recursive$B as ref$P, dereference as dereference$O
        WHERE dereference$N.p1 = ref$P.p1 AND ref$P.p2 = dereference$O.p1))),
      recursive$C AS
        ((SELECT assign$H.p2 as p1, assign$H.p2 as p2 FROM assign as assign$H)
          UNION
        ((SELECT assign$I.p1 as p1, assign$I.p1 as p2 FROM assign as assign$I)
          UNION
        (SELECT ref$Q.p2 as p1, ref$R.p2 as p2
        FROM recursive$A as ref$Q, recursive$A as ref$R
        WHERE ref$Q.p1 = ref$R.p1)
          UNION
        (SELECT ref$S.p2 as p1, ref$T.p2 as p2
        FROM recursive$A as ref$S, recursive$C as ref$U, recursive$A as ref$T
        WHERE ref$S.p1 = ref$U.p1 AND ref$T.p1 = ref$U.p2)))
    SELECT * FROM recursive$A as recref$V
    """
}

type FibNum = (recursionDepth: Int, fibonacciNumber: Int, nextNumber: Int)
type FibNumDB = (base: FibNum, result: FibNum)

given FibNumDBs: TestDatabase[FibNumDB] with
  override def tables = (
    base = Table[FibNum]("base"),
    result = Table[FibNum]("result")
  )

  override def init(): String = """
    CREATE TABLE base (
      recursionDepth INT,
      fibonacciNumber INT,
      nextNumber INT
    );
    INSERT INTO base (recursionDepth, fibonacciNumber, nextNumber) VALUES (0, 0, 1);
  """
class RecursionFibTest extends SQLStringQueryTest[FibNumDB, (FibonacciNumberIndex: Int, FibonacciNumber: Int)] {
  def testDescription: String = "Fibonacci example from duckdb docs"

  def query() =
    val fib = testDB.tables.base
    fib.fix(fib =>
      fib
        .filter(f => (f.recursionDepth + 1) < 10)
        .map(f =>
          (
            recursionDepth = f.recursionDepth + 1,
            fibonacciNumber = f.nextNumber,
            nextNumber = f.fibonacciNumber + f.nextNumber
          ).toRow
        )
        .distinct
    ).map(f => (FibonacciNumberIndex = f.recursionDepth, FibonacciNumber = f.fibonacciNumber).toRow)
  def expectedQueryPattern: String =
    """
    WITH RECURSIVE
      recursive$1 AS
        ((SELECT * FROM base as base$1)
            UNION
        ((SELECT
            ref$0.recursionDepth + 1 as recursionDepth, ref$0.nextNumber as fibonacciNumber, ref$0.fibonacciNumber + ref$0.nextNumber as nextNumber
         FROM recursive$1 as ref$0
         WHERE ref$0.recursionDepth + 1 < 10)))
    SELECT recref$0.recursionDepth as FibonacciNumberIndex, recref$0.fibonacciNumber as FibonacciNumber FROM recursive$1 as recref$0
      """
}

type Tag = (id: Int, name: String, subclassof: Int)
type TagHierarchy = (id: Int, source: String, path: List[String])
type TagDB = (tag: Tag, hierarchy: TagHierarchy)

given TagDBs: TestDatabase[TagDB] with
  override def tables = (
    tag = Table[Tag]("tag"),
    hierarchy = Table[TagHierarchy]("hierarchy")
  )

  override def init(): String = """
    CREATE TABLE tag (id INTEGER, name VARCHAR, subclassof INTEGER);
    INSERT INTO tag VALUES
      (1, 'U2',     5),
      (2, 'Blur',   5),
      (3, 'Oasis',  5),
      (4, '2Pac',   6),
      (5, 'Rock',   7),
      (6, 'Rap',    7),
      (7, 'Music',  9),
      (8, 'Movies', 9),
      (9, 'Art',    NULL);
    """

class RecursionTreeTest extends SQLStringQueryTest[TagDB, List[String]] {
  def testDescription: String = "Tag tree example from duckdb docs"

  def query() =
    import Expr.{toRow, toExpr}
    val tagHierarchy0 = testDB.tables.tag
      .filter(t => t.subclassof.isNull)
      .map(t =>
        val initListPath: Expr.ListExpr[String] = List(t.name).toExpr
        (id = t.id, source = t.name, path = initListPath).toRow
      )
    tagHierarchy0.fix(tagHierarchy1 =>
      tagHierarchy1.flatMap(hier =>
        testDB.tables.tag
          .filter(t => t.subclassof == hier.id)
          .map(t =>
            val listPath = hier.path.prepend(t.name)
            (id = t.id, source = t.name, path = listPath).toRow
          )
      ).distinct
    ).filter(h => h.source == "Oasis").map(h => h.path)
  def expectedQueryPattern: String =
    """
      WITH RECURSIVE
        recursive$62 AS
          ((SELECT
              tag$62.id as id, tag$62.name as source, (ARRAY[tag$62.name]) as path
           FROM tag as tag$62
           WHERE tag$62.subclassof IS NULL)
                UNION
           ((SELECT
              tag$64.id as id, tag$64.name as source, ARRAY_PREPEND(tag$64.name, ref$30.path) as path
            FROM recursive$62 as ref$30, tag as tag$64
            WHERE tag$64.subclassof = ref$30.id)))
      SELECT recref$5.path FROM recursive$62 as recref$5 WHERE recref$5.source = 'Oasis'
      """
}

type Path = (startNode: Int, endNode: Int, path: List[Int])
type ReachabilityDB = (edge: Edge)

given ReachabilityDBs: TestDatabase[ReachabilityDB] with
  override def tables = (
    edge = Table[Edge]("edge")
  )

  override def init(): String = """
    CREATE TABLE edge (x INTEGER, y INTEGER);
    INSERT INTO edge
      VALUES
        (1, 3), (1, 5), (2, 4), (2, 5), (2, 10), (3, 1), (3, 5), (3, 8), (3, 10),
        (5, 3), (5, 4), (5, 8), (6, 3), (6, 4), (7, 4), (8, 1), (9, 4);
    """
class RecursionShortestPathTest extends SQLStringQueryTest[ReachabilityDB, Path] {
  def testDescription: String = "Shortest path example from duckdb docs"

  def query() =
    val pathBase = testDB.tables.edge
      .map(e => (startNode = e.x, endNode = e.y, path = List(e.x, e.y).toExpr).toRow)
      .filter(p => p.startNode == 1) // Filter after map means subquery

    pathBase.fix(path =>
      path.flatMap(p =>
        testDB.tables.edge
          .filter(e =>
            e.x == p.endNode && path.filter(p2 => p2.path.containsElement(e.y)).isEmpty
          )
          .map(e =>
            (startNode = p.startNode, endNode = e.y, path = p.path.append(e.y)).toRow
          )
      ).distinct
    ).sort(p => p.path, Ord.ASC).sort(p => p.path.length, Ord.ASC)

  def expectedQueryPattern: String =
    """
      WITH RECURSIVE
          recursive$116 AS
            ((SELECT * FROM
              (SELECT edge$116.x as startNode, edge$116.y as endNode, (ARRAY[edge$116.x, edge$116.y]) as path
               FROM edge as edge$116) as subquery$117
             WHERE subquery$117.startNode = 1)
                UNION
            ((SELECT
                ref$58.startNode as startNode, edge$118.y as endNode, ARRAY_APPEND(ref$58.path, edge$118.y) as path
             FROM recursive$116 as ref$58, edge as edge$118
             WHERE edge$118.x = ref$58.endNode
                AND
             NOT EXISTS (SELECT * FROM recursive$116 as ref$60 WHERE edge$118.y = ANY(ref$60.path)))))
      SELECT * FROM recursive$116 as recref$9 ORDER BY ARRAY_LENGTH(recref$9.path) ASC, path ASC
      """
}

// Some tests adapted from https://rasql.org/#

class RecursionCPTest extends SQLStringQueryTest[TCDB, (dst: Int, sum: Int)] {
  def testDescription: String = "Count paths"

  def query() =
    val base = testDB.tables.edges.map(e => (y = IntLit(1), cnt = IntLit(1)).toRow)
    base.fix(cp =>
      testDB.tables.edges.flatMap(edge =>
        cp
          .filter(cpaths => cpaths.y == edge.x)
          .map(cpaths => (y = edge.y, cnt = cpaths.cnt).toRow)
      ).distinct
    ).groupBy(s => (dst = s.y).toRow, s => (dst = s.y, sum = sum(s.cnt)).toRow)

  def expectedQueryPattern: String =
    """
         WITH RECURSIVE
           recursive$137 AS
              ((SELECT 1 as y, 1 as cnt FROM edges as edges$137)
                UNION
               ((SELECT
                  edges$139.y as y, ref$65.cnt as cnt
                 FROM edges as edges$139, recursive$137 as ref$65
                 WHERE ref$65.y = edges$139.x)))
         SELECT recref$11.y as dst, SUM(recref$11.cnt) as sum FROM recursive$137 as recref$11 GROUP BY recref$11.y
      """
}

type Report = (empl: Int, mgr: Int)
type ManagementDB = (reports: Report)

given ManagementDBs: TestDatabase[ManagementDB] with
  override def tables = (
    reports = Table[Report]("reports")
  )

  override def init(): String = """
    CREATE TABLE report (
      empl INT,
      mgr INT
    );
  """
class RecursionManagementTest extends SQLStringQueryTest[ManagementDB, (mgr: Int, cnt: Int)] {
  def testDescription: String = "Management query to calculate total number of employees managed"

  def query() =
    val base = testDB.tables.reports.map(e => (mgr = e.mgr, cnt = IntLit(1)).toRow)
    base.fix(empCount =>
      testDB.tables.reports.flatMap(report =>
        empCount
          .filter(ec => ec.mgr == report.mgr)
          .map(ec => (mgr = report.mgr, cnt = ec.cnt).toRow)
      ).distinct
    ).groupBy(ec => (mgr = ec.mgr).toRow, ec => (mgr = ec.mgr, cnt = count(ec.cnt)).toRow)

  def expectedQueryPattern: String =
    """
        WITH RECURSIVE
          recursive$157 AS
            ((SELECT reports$157.mgr as mgr, 1 as cnt FROM reports as reports$157)
              UNION
             ((SELECT reports$159.mgr as mgr, ref$77.cnt as cnt
               FROM reports as reports$159, recursive$157 as ref$77
               WHERE ref$77.mgr = reports$159.mgr)))
        SELECT recref$13.mgr as mgr, COUNT(recref$13.cnt) as cnt FROM recursive$157 as recref$13 GROUP BY recref$13.mgr
      """
}

type Sales = (m: Int, p: Double)
type Sponsor = (m1: Int, m2: Int)
type MLMDatabase = (sales: Sales, sponsors: Sponsor)

given MLMDatabases: TestDatabase[MLMDatabase] with
  override def tables = (
    sales = Table[Sales]("sales"),
    sponsors = Table[Sponsor]("sponsors")
  )

  override def init(): String =
    """
      CREATE TABLE sales (
        m INT,
        p DOUBLE
      );

      CREATE TABLE sponsor (
        m1 INT,
        m2 INT
      );
    """

class RecursionMLMBonusTest extends SQLStringQueryTest[MLMDatabase, (m: Int, b: Double)] {
  def testDescription: String = "MLM Bonus query to calculate the bonus distributed to each member"

  def query() =
    val base = testDB.tables.sales.map(s => (m = s.m, b = s.p * 0.1).toRow)
    base.fix(bonus =>
      testDB.tables.sponsors.flatMap(sponsor =>
        bonus
          .filter(b => b.m == sponsor.m2)
          .map(b => (m = sponsor.m1, b = b.b * 0.5).toRow)
      ).distinct
    ).groupBy(r => (m = r.m).toRow, r => (m = r.m, b = sum(r.b)).toRow)

  def expectedQueryPattern: String =
    """
        WITH RECURSIVE
          recursive$240 AS
            ((SELECT sales$240.m as m, sales$240.p * 0.1 as b FROM sales as sales$240)
              UNION
            ((SELECT sponsors$242.m1 as m, ref$121.b * 0.5 as b
             FROM sponsors as sponsors$242, recursive$240 as ref$121
             WHERE ref$121.m = sponsors$242.m2)))
       SELECT recref$20.m as m, SUM(recref$20.b) as b FROM recursive$240 as recref$20 GROUP BY recref$20.m
      """
}

type Interval = (s: Int, e: Int)
type IntervalDB = (intervals: Interval)

given IntervalDBs: TestDatabase[IntervalDB] with
  override def tables = (
    intervals = Table[Interval]("inter")
  )

  override def init(): String =
    """
      CREATE TABLE intervals (
        s INT,
        e INT
      );
    """

/** TODO: figure out how to do groupBy on join result. Right now have unimplemented "mergeMap" method but probably
  * better to extract all source relations from source query and then supply them as arguments to the function arguments
  * of groupBy, so the project/groupBy/having functions can access the original, non-aggregated relations.
  */
//class RecursionIntervalCoalesceTest extends SQLStringQueryTest[IntervalDB, (s: Int, e: Int)] {
//  def testDescription: String = "Interval Coalesce query to find the smallest set of intervals that cover input intervals"
//
//  def query() =
//    // Step 1: Create the non-recursive view `lstart`
//    val lstart = testDB.tables.intervals
//      .flatMap(a =>
//        testDB.tables.intervals
//          .filter(b => a.s <= b.e)
//          .map(b => (as = a.s, bs = b.s).toRow))
//      .filterByGroupBy(
//        row => (t = row.as).toRow,
//        row => (t = row.as).toRow,
//        rows => rows.as == min(rows.bs)
//      )
//
//    // Step 2: Define the recursive `coal` relation
//    val base = lstart.flatMap(ls =>
//      testDB.tables.intervals
//        .filter(int => ls.t == int.s)
//        .map(int => (s = ls.t, e = int.e))
//    )
//
//    base.fix(coal =>
//      testDB.tables.intervals.flatMap(inter =>
//        coal
//          .filter(c => c.s <= inter.s && inter.s <= c.e)
//          .map(c => (s = c.s, e = inter.e).toRow)
//      ).distinct
//    ).groupBy(c => (s = c.s).toRow, c => (s = c.s, e = max(c.e)).toRow)
//
//  def expectedQueryPattern: String =
//    """
//      CREATE VIEW lstart AS
//        (SELECT a.s AS t
//         FROM inter a, inter b
//         WHERE a.s <= b.e
//         GROUP BY a.s
//         HAVING a.s = MIN(b.s));
//
//      WITH RECURSIVE
//        coal (s, e) AS
//          ((SELECT lstart.t AS s, inter.e AS e
//            FROM lstart, inter
//            WHERE lstart.t = inter.s)
//            UNION
//           (SELECT coal.s, inter.e
//            FROM coal, inter
//            WHERE coal.s <= inter.s AND inter.s <= coal.e))
//      SELECT coalref.s AS s, MAX(coalref.e) AS e
//      FROM coal as coalref
//      GROUP BY coalref.s
//    """
//}

class MinReachTest extends SQLStringQueryTest[TCDB, (x: Int, min_y: Int)] {
  def testDescription: String = "Minimum reachability using aggregation at the end"

  def query() =
    val reach = testDB.tables.edges
    reach.fix(reach =>
      reach
        .flatMap(p =>
          testDB.tables.edges
            .filter(e => p.y == e.x)
            .map(e => (x = p.x, y = e.y).toRow)
        )
        .distinct
    ).groupBy(
      row => (x = row.x).toRow,
      row => (x = row.x, min_y = min(row.y)).toRow
    )

  def expectedQueryPattern: String =
    """
    WITH RECURSIVE recursive$R AS
      ((SELECT * FROM edges as edges$A)
        UNION
      ((SELECT ref$P.x as x, edges$B.y as y
       FROM recursive$R as ref$P, edges as edges$B
       WHERE ref$P.y = edges$B.x)))
    SELECT recref$F.x as x, MIN(recref$F.y) as min_y FROM recursive$R as recref$F GROUP BY recref$F.x
    """
}

class MinReachStratifiedTest extends SQLStringQueryTest[TCDB, (x: Int, min_y: Int)] {
  def testDescription: String = "Minimum reachability using aggregation across strata"

  def query() =
    val edges = testDB.tables.edges.groupBy(e => (x = e.x).toRow, e => (x = e.x, y = min(e.y)).toRow)

    edges.fix(minReach =>
      minReach.flatMap(mr =>
        edges
          .filter(e => mr.y == e.x)
          .map(e => (x = mr.x, y = e.y).toRow)
      ).distinct
    ).groupBy(
      row => (x = row.x).toRow,
      row => (x = row.x, min_y = min(row.y)).toRow
    )

  def expectedQueryPattern: String =
    """
    WITH RECURSIVE recursive$A AS
    ((SELECT edges$B.x as x, MIN(edges$B.y) as y
      FROM edges as edges$B
      GROUP BY edges$B.x)
    UNION
      ((SELECT ref$C.x as x, subquery$D.y as y
      FROM recursive$A as ref$C,
           (SELECT edges$E.x as x, MIN(edges$E.y) as y
             FROM edges as edges$E
             GROUP BY edges$E.x) as subquery$D
      WHERE ref$C.y = subquery$D.x)))
  SELECT recref$F.x as x, MIN(recref$F.y) as min_y
  FROM recursive$A as recref$F
  GROUP BY recref$F.x
    """
}

type ParentChildDB = (parentChild: Edge)

given ParentChildDBs: TestDatabase[ParentChildDB] with
  override def tables = (
    parentChild = Table[Edge]("parentChild")
  )

  override def init(): String =
    """
      CREATE TABLE parentChild (
        x INT,
        y INT
      );

      INSERT INTO parentChild (x, y) VALUES (1, 2);
      INSERT INTO parentChild (x, y) VALUES (2, 3);
      INSERT INTO parentChild (x, y) VALUES (3, 4);
      INSERT INTO parentChild (x, y) VALUES (4, 5);
      INSERT INTO parentChild (x, y) VALUES (2, 6);
      """

class AncestorRecursiveTest extends SQLStringQueryTest[ParentChildDB, (x: Int, y: Int)] {
  def testDescription: String = "recursive ancestor-descendant relationship"

  def query() =
    val parentChild = testDB.tables.parentChild

    val ancestorBase = parentChild
    val descendantBase = parentChild

    val (ancestorResult, descendantResult) = fix(ancestorBase, descendantBase) { (ancestor, descendant) =>
      val newAncestor = ancestor.flatMap(a =>
        parentChild
          .filter(p => a.y == p.x)
          .map(p => (x = a.x, y = p.y).toRow)
      )

      val newDescendant = descendant.flatMap(d =>
        parentChild
          .filter(p => d.x == p.y)
          .map(p => (x = d.y, y = p.x).toRow)
      )
      (newAncestor.distinct, newDescendant.distinct)
    }

    ancestorResult

  def expectedQueryPattern: String =
    """
    WITH RECURSIVE
      recursive$319 AS
        ((SELECT * FROM parentChild as parentChild$320)
          UNION
        ((SELECT ref$156.x as x, parentChild$322.y as y
          FROM recursive$319 as ref$156, parentChild as parentChild$322
          WHERE ref$156.y = parentChild$322.x))),
      recursive$321 AS
        ((SELECT * FROM parentChild as parentChild$326)
          UNION
        ((SELECT ref$159.y as x, parentChild$328.x as y
          FROM recursive$321 as ref$159, parentChild as parentChild$328
          WHERE ref$159.x = parentChild$328.y)))
    SELECT * FROM recursive$319 as recref$28
      """
}

type FriendshipDB = (friendship: Edge)

given FriendshipDBs: TestDatabase[FriendshipDB] with
  override def tables = (
    friendship = Table[Edge]("friendship")
  )

  override def init(): String =
    """
      CREATE TABLE friendship (
        x INT,
        y INT
      );

      INSERT INTO friendship (x, y) VALUES (1, 2);
      INSERT INTO friendship (x, y) VALUES (2, 3);
      INSERT INTO friendship (x, y) VALUES (3, 4);
      INSERT INTO friendship (x, y) VALUES (4, 5);
      INSERT INTO friendship (x, y) VALUES (2, 6);
      """

class MutualFriendsStratifiedTest extends SQLStringQueryTest[FriendshipDB, (x: Int, mutual_friend_count: Int)] {
  def testDescription: String = "Mutually recursive query with stratified aggregation. NOTE: only duckdb"

  def query() =
    val baseFriendships = testDB.tables.friendship

    // Stratum 1
    val directFriendsCount = baseFriendships.groupBy(
      f => (x = f.x).toRow,
      f => (x = f.x, friend_count = count(f.y)).toRow
    )

    // Define the mutually recursive fixpoint
    val (totalFriendsResult, mutualFriendsResult) =
      fix(directFriendsCount, baseFriendships) { (totalFriendsCount, friendships) =>
        val recurTotalFriendsCount = totalFriendsCount.flatMap(tf1 =>
          friendships
            .filter(f => tf1.x == f.x)
            .flatMap(f =>
              directFriendsCount
                .filter(dfCount => f.y == dfCount.x)
                .map(dfCount => (x = tf1.x, friend_count = dfCount.friend_count).toRow)
            )
        ).distinct

        // Define the mutually recursive MutualFriends relation
        val recurFriendships = totalFriendsCount.flatMap(tfCount1 =>
          friendships
            .filter(f => tfCount1.x <= f.x)
            .map(f => (x = tfCount1.x, y = f.x).toRow)
        )

        (recurTotalFriendsCount, recurFriendships.distinct)
      }
    mutualFriendsResult.groupBy(
      row => (x = row.x).toRow,
      row => (x = row.x, mutual_friend_count = count(row.y)).toRow
    )

  def expectedQueryPattern: String =
    """
      WITH RECURSIVE
        recursive$271 AS
          ((SELECT friendship$272.x as x, COUNT(friendship$272.y) as friend_count
           FROM friendship as friendship$272
           GROUP BY friendship$272.x)
              UNION
          ((SELECT ref$135.x as x, subquery$277.friend_count as friend_count
           FROM recursive$271 as ref$135, recursive$272 as ref$136,
              (SELECT friendship$275.x as x, COUNT(friendship$275.y) as friend_count
               FROM friendship as friendship$275
               GROUP BY friendship$275.x) as subquery$277
           WHERE (ref$135.x = ref$136.x AND ref$136.y = subquery$277.x)))),
         recursive$272 AS
          ((SELECT * FROM friendship as friendship$282)
            UNION
          ((SELECT ref$140.x as x, ref$141.x as y
            FROM recursive$271 as ref$140, recursive$272 as ref$141
            WHERE ref$140.x <= ref$141.x)))
      SELECT recref$24.x as x, COUNT(recref$24.y) as mutual_friend_count FROM recursive$272 as recref$24 GROUP BY recref$24.x
    """
}

/* Currently passes, but by the definition of fix, should not define non-mutually recursive relation using fix
   (should be done outside of the function like in the previous test). However, would be nice to add a restriction to enforce this.*/
class MutualFriendsStratifiedFutureFailTest
    extends SQLStringQueryTest[FriendshipDB, (x: Int, mutual_friend_count: Int)] {
  def testDescription: String =
    "Mutually recursive query with stratified aggregation. Here define within recur the first result. Technically against the spec because by definition all arguments to fix must be mutually recursive."

  def query() =
    val baseFriendships = testDB.tables.friendship

    // Stratum 1
    val directFriendsCount = baseFriendships.groupBy(
      f => (x = f.x).toRow,
      f => (x = f.x, friend_count = count(f.y)).toRow
    )

    // Define the mutually recursive fixpoint
    val (dfC, totalFriendsResult, mutualFriendsResult) =
      fix(directFriendsCount, directFriendsCount, baseFriendships) { (dfC2, totalFriendsCount, friendships) =>
        val recurTotalFriendsCount = totalFriendsCount.flatMap(tf1 =>
          friendships
            .filter(f => tf1.x == f.x)
            .flatMap(f =>
              dfC2
                .filter(dfCount => f.y == dfCount.x)
                .map(dfCount => (x = tf1.x, friend_count = dfCount.friend_count).toRow)
            )
        ).distinct

        // Define the mutually recursive MutualFriends relation
        val recurFriendships = totalFriendsCount.flatMap(tfCount1 =>
          friendships
            .filter(f => tfCount1.x <= f.x)
            .map(f => (x = tfCount1.x, y = f.x).toRow)
        )

        (dfC2, recurTotalFriendsCount, recurFriendships.distinct)
      }
    mutualFriendsResult.groupBy(
      row => (x = row.x).toRow,
      row => (x = row.x, mutual_friend_count = count(row.y)).toRow
    )

  def expectedQueryPattern: String =
    """
      WITH RECURSIVE
        recursive$187 AS
          ((SELECT friendship$189.x as x, COUNT(friendship$189.y) as friend_count
            FROM friendship as friendship$189
            GROUP BY friendship$189.x)
              UNION
          ((SELECT * FROM recursive$187 as recref$16))),
        recursive$188 AS
          ((SELECT friendship$194.x as x, COUNT(friendship$194.y) as friend_count
            FROM friendship as friendship$194
            GROUP BY friendship$194.x)
              UNION
           ((SELECT ref$91.x as x, ref$94.friend_count as friend_count
             FROM recursive$188 as ref$91, recursive$189 as ref$92, recursive$187 as ref$94
             WHERE (ref$91.x = ref$92.x AND ref$92.y = ref$94.x)))),
        recursive$189 AS
          ((SELECT * FROM friendship as friendship$201)
              UNION
          ((SELECT ref$96.x as x, ref$97.x as y
            FROM recursive$188 as ref$96, recursive$189 as ref$97
            WHERE ref$96.x <= ref$97.x)))
     SELECT recref$18.x as x, COUNT(recref$18.y) as mutual_friend_count FROM recursive$189 as recref$18 GROUP BY recref$18.x
      """
}

type CyclicEdge = (x: Int, y: Int)
type CyclicGraphDB = (edges: CyclicEdge)

given CyclicGraphDBs: TestDatabase[CyclicGraphDB] with
  override def tables = (
    edges = Table[CyclicEdge]("edges")
  )

  override def init(): String =
    """
      CREATE TABLE edges (
        x INT,
        y INT
      );

      INSERT INTO edges (x, y) VALUES (1, 2);
      INSERT INTO edges (x, y) VALUES (2, 3);
      INSERT INTO edges (x, y) VALUES (3, 1);
    """

class RecursiveNonterminationExampleTest extends SQLStringQueryTest[CyclicGraphDB, CyclicEdge] {
  def testDescription: String = "Recursive query that cycles indefinitely under bag semantics"

  def query() =
    val base = testDB.tables.edges
    base.fix(path =>
      path.flatMap(p =>
        testDB.tables.edges
          .filter(e => p.y == e.x)
          .map(e => (x = p.x, y = e.y).toRow)
      ).distinct // Removing 'distinct' will cause it to never terminate
    )

  def expectedQueryPattern: String =
    """
      WITH RECURSIVE recursive$A AS
        ((SELECT * FROM edges as edges$B)
          UNION
        ((SELECT ref$R.x as x, edges$E.y as y
         FROM recursive$A as ref$R, edges as edges$E
         WHERE ref$R.y = edges$E.x)))
      SELECT * FROM recursive$A as recref$Z
    """
}

type Orbits = (x: String, y: String)
type PlanetaryDB = (orbits: Orbits, base: Orbits, intermediate: Orbits)

given PlanetaryDBs: TestDatabase[PlanetaryDB] with
  override def tables = (
    orbits = Table[Orbits]("orbits"),
    base = Table[Orbits]("base"),
    intermediate = Table[Orbits]("intermediate")
  )

  override def init(): String =
    """
      CREATE TABLE orbits (
          x TEXT,
          y TEXT
      );

      INSERT INTO orbits (x, y) VALUES
      ('Earth', 'Sun'),
      ('Moon', 'Earth'),
      ('ISS', 'Earth'),
      ('Mars', 'Sun'),
      ('Phobos', 'Mars'),
      ('Deimos', 'Mars');
      """

class RecursiveOrbitsTest extends SQLStringQueryTest[PlanetaryDB, Orbits] {
  def testDescription: String = "Planetary orbits from souffle benchmark"

  def query() =
    val base = testDB.tables.base
    val orbits = unrestrictedFix(Tuple1(base))(orbitsT =>
      val orbits = orbitsT._1
      val res = orbits.flatMap(p =>
        orbits
          .filter(e => p.y == e.x)
          .map(e => (x = p.x, y = e.y).toRow)
      ).distinct
      Tuple1(res)
    )._1

    val orbitsRef = orbits.$resultQuery

    orbits.filter(o =>
      orbitsRef.flatMap(o1 =>
        orbitsRef
          .filter(o2 => o1.y == o2.x)
          .map(o2 => (x = o1.x, y = o2.y).toRow)
      ).filter(io => o.x == io.x && o.y == io.y)
        .isEmpty
    )

  def expectedQueryPattern: String =
    """
        WITH RECURSIVE
          recursive$1 AS
            ((SELECT * FROM base as base$1)
              UNION
            ((SELECT ref$0.x as x, ref$1.y as y
              FROM recursive$1 as ref$0, recursive$1 as ref$1
              WHERE ref$0.y = ref$1.x)))
        SELECT *
        FROM recursive$1 as recref$0
        WHERE NOT EXISTS
          (SELECT * FROM
            (SELECT ref$4.x as x, ref$5.y as y
            FROM recursive$1 as ref$4, recursive$1 as ref$5
            WHERE ref$4.y = ref$5.x) as subquery$9
          WHERE recref$0.x = subquery$9.x AND recref$0.y = subquery$9.y)
    """

}
