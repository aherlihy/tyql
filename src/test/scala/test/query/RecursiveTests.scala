package test.query.recursive
import test.{SQLStringAggregationTest, SQLStringQueryTest, TestDatabase}
import tyql.*
import Query.{fix, unrestrictedFix}
import Expr.{max, min, IntLit, sum}

import language.experimental.namedTuples
import NamedTuple.*
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

// TODO: should this error?
//class NotRecursiveCTETest extends SQLStringQueryTest[TCDB, Edge] {
//  def testDescription: String = "No recursion"
//
//  def query() =
//    val path = testDB.tables.edges
//    path.fix(path =>
//      testDB.tables.edges
//    )
//  def expectedQueryPattern: String =
//    """
//    WITH RECURSIVE recursive$A AS
//      (SELECT * FROM edges as edges$B
//        UNION
//      SELECT * FROM edges as edges$E)
//    SELECT * FROM recursive$A as recref$Z
//      """
//}

//class RecursiveSCCTest extends SQLStringQueryTest[TCDB, Edge2] {
//  def testDescription: String = "Multi-relation recursion"
//
//  def query() =
//    val path1 = testDB.tables.edges
//    val path2 = testDB.tables.otherEdges
//
//    // Option 1: untupled
//    val (fullPath1, fullPath2) = fixUntupled((p: Query[Edge], q: Query[Edge2]) =>
//      (p, q)
//    )(path1, path2)
//
//    // Option 2: tupled
//    val (fullPath1a, fullPath2a) = fix((t: (Query[Edge], Query[Edge2])) =>
//      (t._1, t._2)
//    )((path1, path2))
//
//    // Option 3: static tuple length
//    val (fullPath1b, fullPath2b) = fixTwo(path1, path2)((p, q) =>
//      (p, q)
//    )
//
//    fullPath2
//  def expectedQueryPattern: String =
//    """
//    """
//}


type Location = (p1: Int, p2: Int)
type CSPADB = (assign: Location, dereference: Location, empty: Location)

given CSPADBs: TestDatabase[CSPADB] with
  override def tables = (
    assign = Table[Location]("assign"),
    dereference = Table[Location]("dereference"),
    empty = Table[Location]("empty") // TODO: define singleton for empty table?
  )

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

    val (valueFlowFinal, valueAliasFinal, memoryAliasFinal) = unrestrictedFix(valueFlowBase, testDB.tables.empty, memoryAliasBase)(
      (valueFlow, valueAlias, memoryAlias) =>
        val VF =
          // ValueFlow(x, y) :- (Assign(x, z), MemoryAlias(z, y))
          assign.flatMap(a =>
            memoryAlias
              .filter(m => a.p2 == m.p1)
              .map(m => (p1 = a.p1, p2 = m.p2).toRow
              )
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

    val (valueFlowFinal, valueAliasFinal, memoryAliasFinal) = unrestrictedFix(valueFlowBase, testDB.tables.empty, memoryAliasBase)(
      (valueFlow, valueAlias, memoryAlias) =>
        val VF =
          // ValueFlow(x, y) :- (Assign(x, z), MemoryAlias(z, y))
          assign.flatMap(a =>
            memoryAlias
              .filter(m => a.p2 == m.p1)
              .map(m => (p1 = a.p1, p2 = m.p2).toRow
            )
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

class RecursiveCSPAComprehensionTest extends SQLStringQueryTest[CSPADB, Location] {
  def testDescription: String = "CSPA, but with comprehensions to see if nicer"
  def dbSetup: String = """
    CREATE TABLE assign (
      p1 INT,
      p2 INT
    );
    CREATE TABLE dereference (
      p1 INT,
      p2 INT
    );
  """

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

    val (valueFlowFinal, valueAliasFinal, memoryAliasFinal) = unrestrictedFix(valueFlowBase, testDB.tables.empty, memoryAliasBase)(
      (valueFlow, valueAlias, memoryAlias) =>
        // ValueFlow(x, y) :- (Assign(x, z), MemoryAlias(z, y))
        val vfDef1 =
          for
            a <- assign
            m <- memoryAlias
            if a.p2 == m.p1
          yield (p1 = a.p1, p2 = m.p2).toRow
        // ValueFlow(x, y) :- (ValueFlow(x, z), ValueFlow(z, y))
        val vfDef2 =
          for
            vf1 <- valueFlow
            vf2 <- valueFlow
            if vf1.p2 == vf2.p1
          yield (p1 = vf1.p1, p2 = vf2.p2).toRow
        val VF = vfDef1.union(vfDef2)

        // MemoryAlias(x, w) :- (Dereference(y, x), ValueAlias(y, z), Dereference(z, w))
        val MA =
          for
            d1 <- dereference
            va <- valueAlias
            d2 <- dereference
            if d1.p1 == va.p1 && va.p2 == d2.p1
          yield (p1 = d1.p2, p2 = d2.p2).toRow

        // ValueAlias(x, y) :- (ValueFlow(z, x), ValueFlow(z, y))
        val vaDef1 =
          for
            vf1 <- valueFlow
            vf2 <- valueFlow
            if vf1.p1 == vf2.p1
          yield (p1 = vf1.p2, p2 = vf2.p2).toRow
        // ValueAlias(x, y) :- (ValueFlow(z, x), MemoryAlias(z, w), ValueFlow(w, y))
        val vaDef2 =
          for
            vf1 <- valueFlow
            m <- memoryAlias
            vf2 <- valueFlow
            if vf1.p1 == m.p1 && vf2.p1 == m.p2
          yield (p1 = vf1.p2, p2 = vf2.p2).toRow
        val VA = vaDef1.union(vaDef2)

        (VF, MA.distinct, VA)
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
class RecursionFibTest extends SQLStringQueryTest[FibNumDB, (FibonacciNumberIndex: Int, FibonacciNumber: Int)] {
  def testDescription: String = "Fibonacci example from duckdb docs"

  def dbSetup: String = """
    CREATE TABLE base (
      recursionDepth INT,
      fibonacciNumber INT,
      nextNumber INT
    );
    INSERT INTO base (recursionDepth, fibonacciNumber, nextNumber) VALUES (0, 0, 1);
  """
  def query() =
    val fib = testDB.tables.base
    fib.fix(fib =>
      fib
        .filter(f => (f.recursionDepth + 1) < 10)
        .map(f => (recursionDepth = f.recursionDepth + 1, fibonacciNumber = f.nextNumber, nextNumber = f.fibonacciNumber + f.nextNumber).toRow)
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

class RecursionTreeTest extends SQLStringQueryTest[TagDB, List[String]] {
  def testDescription: String = "Tag tree example from duckdb docs"
  def dbSetup: String =
    """
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
      (9, 'Art', -1);
    """

  def query() =
    // For now encode NULL as -1, TODO: implement nulls
    import Expr.{toRow, toExpr}
    val tagHierarchy0 = testDB.tables.tag
      .filter(t => t.subclassof == -1)
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
              tag$62.id as id, tag$62.name as source, [tag$62.name] as path
           FROM tag as tag$62
           WHERE tag$62.subclassof = -1)
                UNION
           ((SELECT
              tag$64.id as id, tag$64.name as source, list_prepend(tag$64.name, ref$30.path) as path
            FROM recursive$62 as ref$30, tag as tag$64
            WHERE tag$64.subclassof = ref$30.id)))
      SELECT recref$5.path FROM recursive$62 as recref$5 WHERE recref$5.source = "Oasis"
      """
}

type Path = (startNode: Int, endNode: Int, path: List[Int])
type ReachabilityDB = (edge: Edge)

given ReachabilityDBs: TestDatabase[ReachabilityDB] with
  override def tables = (
    edge = Table[Edge]("edge")
  )
class RecursionReachabilityTest extends SQLStringQueryTest[ReachabilityDB, Path] {
  def testDescription: String = "Enumerate all paths from a node example from duckdb docs"
  def dbSetup: String =
    """
    CREATE TABLE edge (x INTEGER, y INTEGER);
    INSERT INTO edge
      VALUES
        (1, 3), (1, 5), (2, 4), (2, 5), (2, 10), (3, 1), (3, 5), (3, 8), (3, 10),
        (5, 3), (5, 4), (5, 8), (6, 3), (6, 4), (7, 4), (8, 1), (9, 4);
    """

  def query() =
    val pathBase = testDB.tables.edge
      .map(e => (startNode = e.x, endNode = e.y, path = List(e.x, e.y).toExpr).toRow)
      .filter(p => p.startNode == 1) // Filter after map means subquery

    pathBase.fix(path =>
      path.flatMap(p =>
        testDB.tables.edge
          .filter(e => e.x == p.endNode && !p.path.contains(e.y))
          .map(e =>
            (startNode = p.startNode, endNode = e.y, path = p.path.append(e.y)).toRow
          )
        ).distinct
      ).sort(p => p.path, Ord.ASC).sort(p => p.path.length, Ord.ASC)

  def expectedQueryPattern: String =
    """
      WITH RECURSIVE
        recursive$150 AS
          ((SELECT * FROM (SELECT edge$150.x as startNode, edge$150.y as endNode, [edge$150.x, edge$150.y] as path
           FROM edge as edge$150) as subquery$151
          WHERE subquery$151.startNode = 1)
            UNION
          ((SELECT ref$70.startNode as startNode, edge$152.y as endNode, list_append(ref$70.path, edge$152.y) as path
           FROM recursive$150 as ref$70, edge as edge$152
           WHERE edge$152.x = ref$70.endNode AND NOT list_contains(ref$70.path, edge$152.y))))
      SELECT * FROM recursive$150 as recref$13 ORDER BY length(recref$13.path) ASC, path ASC
      """
}
class RecursionShortestPathTest extends SQLStringQueryTest[ReachabilityDB, Path] {
  def testDescription: String = "Shortest path example from duckdb docs"
  def dbSetup: String =
    """
    CREATE TABLE edge (x INTEGER, y INTEGER);
    INSERT INTO edge
      VALUES
        (1, 3), (1, 5), (2, 4), (2, 5), (2, 10), (3, 1), (3, 5), (3, 8), (3, 10),
        (5, 3), (5, 4), (5, 8), (6, 3), (6, 4), (7, 4), (8, 1), (9, 4);
    """

  def query() =
    val pathBase = testDB.tables.edge
      .map(e => (startNode = e.x, endNode = e.y, path = List(e.x, e.y).toExpr).toRow)
      .filter(p => p.startNode == 1) // Filter after map means subquery

    pathBase.fix(path =>
      path.flatMap(p =>
        testDB.tables.edge
          .filter(e =>
            e.x == p.endNode && path.filter(p2 => p2.path.contains(e.y)).isEmpty
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
              (SELECT edge$116.x as startNode, edge$116.y as endNode, [edge$116.x, edge$116.y] as path
               FROM edge as edge$116) as subquery$117
             WHERE subquery$117.startNode = 1)
                UNION
            ((SELECT
                ref$58.startNode as startNode, edge$118.y as endNode, list_append(ref$58.path, edge$118.y) as path
             FROM recursive$116 as ref$58, edge as edge$118
             WHERE edge$118.x = ref$58.endNode
                AND
             NOT EXISTS (SELECT * FROM recursive$116 as ref$60 WHERE list_contains(ref$60.path, edge$118.y)))))
      SELECT * FROM recursive$116 as recref$9 ORDER BY length(recref$9.path) ASC, path ASC
      """
}

// Some tests adapted from https://rasql.org/#
type Assbl = (part: String, spart: String)
type Basic = (part: String, days: Int)

type BOMDB = (assbl: Assbl, basic: Basic)

given BOMDBs: TestDatabase[BOMDB] with
  override def tables = (
    assbl = Table[Assbl]("assbl"),
    basic = Table[Basic]("basic")
  )

class RecursionBOMTest extends SQLStringAggregationTest[BOMDB, Int] {
  def testDescription: String = "BOM 'days til delivery' stratified with final aggregation"

  def query() =
    val waitFor = testDB.tables.basic
    waitFor.fix(waitFor =>
      testDB.tables.assbl.flatMap(assbl =>
        waitFor
          .filter(wf => assbl.spart == wf.part)
          .map(wf => (part = assbl.part, days = wf.days).toRow)
      ).distinct
    ).aggregate(wf => max(wf.days)) // .groupBy(wf.part, wf => (part = wf.part, max = max(wf.days))

  def expectedQueryPattern: String =
    """
        WITH RECURSIVE
          recursive$178 AS
            ((SELECT * FROM basic as basic$178)
              UNION
              ((SELECT
                  assbl$180.part as part, ref$89.days as days
                FROM assbl as assbl$180, recursive$178 as ref$89
                WHERE assbl$180.spart = ref$89.part)))
        SELECT MAX(recref$14.days) FROM recursive$178 as recref$14
      """
}

type SSSPEdge = (src: Int, dst: Int, cost: Double)
type SSSPBase = (dst: Int, cost: Double)
type SSSPDB = (edge: SSSPEdge, base: SSSPBase)

given SSSPDBs: TestDatabase[SSSPDB] with
  override def tables = (
    edge = Table[SSSPEdge]("edge"),
    base = Table[SSSPBase]("base") // e.g. (0, 1)
  )

class RecursionSSSPTest extends SQLStringAggregationTest[SSSPDB, Double] {
  def testDescription: String = "Single source shortest path"

  def query() =
    val base = testDB.tables.base
    base.fix(sp =>
      testDB.tables.edge.flatMap(edge =>
        sp
          .filter(s => s.dst == edge.src)
          .map(s => (dst = edge.dst, cost = s.cost + edge.cost).toRow)
      ).distinct
    ).aggregate(s => min(s.cost)) // .aggregate(s => (dst = s.dst, cost = min(s.cost)))

  def expectedQueryPattern: String =
    """
        WITH RECURSIVE
          recursive$62 AS
            ((SELECT * FROM base as base$62)
                UNION
              ((SELECT
                  edge$64.dst as dst, ref$29.cost + edge$64.cost as cost
                FROM edge as edge$64, recursive$62 as ref$29
                WHERE ref$29.dst = edge$64.src)))
        SELECT MIN(recref$5.cost) FROM recursive$62 as recref$5
      """
}

class RecursionCCTest extends SQLStringAggregationTest[TCDB, Int] {
  def testDescription: String = "Connected components"

  def query() =
    val base = testDB.tables.edges.map(e => (x = e.x, id = e.y))
    base.fix(cc =>
      testDB.tables.edges.flatMap(edge =>
        cc
          .filter(s => s.x == edge.x)
          .map(s => (x = edge.y, id = s.id).toRow)
      ).distinct
    ).map(s => s.id).distinct.size

  def expectedQueryPattern: String =
    """
        SELECT DISTINCT COUNT(1) FROM
          WITH RECURSIVE recursive$53 AS
            ((SELECT
                edges$53.x as x, edges$53.y as id
              FROM edges as edges$53)
                UNION
            ((SELECT
                edges$55.y as x, ref$26.id as id
              FROM edges as edges$55, recursive$53 as ref$26
              WHERE ref$26.x = edges$55.x)))
          SELECT recref$4.id FROM recursive$53 as recref$4
      """
}

class RecursionCPTest extends SQLStringAggregationTest[TCDB, Int] {
  def testDescription: String = "Count paths"

  def query() =
    val base = testDB.tables.edges.map(e => (y = IntLit(1), cnt = IntLit(1)).toRow)
    base.fix(cp =>
      testDB.tables.edges.flatMap(edge =>
        cp
          .filter(cpaths => cpaths.y == edge.x)
          .map(cpaths => (y = edge.y, cnt = cpaths.cnt).toRow)
      ).distinct
    ).aggregate(s => sum(s.cnt)) // .aggregate(s.y, sum(s.cnt))

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
         SELECT SUM(recref$11.cnt) FROM recursive$137 as recref$11
      """
}

type Report = (empl: Int, mgr: Int)
type ManagementDB = (reports: Report)

given ManagementDBs: TestDatabase[ManagementDB] with
  override def tables = (
    reports = Table[Report]("reports")
    )
class RecursionManagementTest extends SQLStringAggregationTest[ManagementDB, Int] {
  def testDescription: String = "Management query to calculate total number of employees managed"

  def query() =
    val base = testDB.tables.reports.map(e => (mgr = e.mgr, cnt = IntLit(1)).toRow)
    base.fix(empCount =>
      testDB.tables.reports.flatMap(report =>
        empCount
          .filter(ec => ec.mgr == report.mgr)
          .map(ec => (mgr = report.mgr, cnt = ec.cnt).toRow)
      ).distinct
    ).size // .aggregate(ec => (mgr = ec.mgr, cnt = count(ec.cnt))

  def expectedQueryPattern: String =
    """
        WITH RECURSIVE
          recursive$157 AS
            ((SELECT reports$157.mgr as mgr, 1 as cnt FROM reports as reports$157)
              UNION
             ((SELECT reports$159.mgr as mgr, ref$77.cnt as cnt
               FROM reports as reports$159, recursive$157 as ref$77
               WHERE ref$77.mgr = reports$159.mgr)))
        SELECT COUNT(1) FROM recursive$157 as recref$13
      """
}

//type Organizer = (orgName: String)
//type Friend = (personName: String, friendName: String)
//type PartyDB = (organizer: Organizer, friends: Friend)
//
//given PartyDBs: TestDatabase[PartyDB] with
//  override def tables = (
//    organizer = Table[Organizer]("organizer"),
//    friends = Table[Friend]("friends")
//  )
//class RecursionPartyTest extends SQLStringAggregationTest[PartyDB, String] {
//  def testDescription: String = "Mututally recursive wh will attend a party query"
//
//  def query() =
//    val baseAttend = testDB.tables.organizer.map(o => (person = o.orgName).toRow)
//    val baseFriends = testDB.tables.friends.map(f => (name = f.personName, nCount = IntLit(0)).toRow)
//
//    fix(baseAttend, baseFriends)((attend, cntfriends) =>
//      val attendRecur = cntfriends.filter(cf => cf.nCount > 3)
//      val countRecur = cntfriends.flatMap(friend =>
//        attend
//          .filter(att => att.person == friend.name)
//          .map(att => friend)
//        )
//      (attendRecur.distinct, countRecur.distinct)
//    )._1.size
//
//  def expectedQueryPattern: String =
//    """
//        WITH RECURSIVE
//          recursive$157 AS
//            ((SELECT reports$157.mgr as mgr, 1 as cnt FROM reports as reports$157)
//              UNION
//             ((SELECT reports$159.mgr as mgr, ref$77.cnt as cnt
//               FROM reports as reports$159, recursive$157 as ref$77
//               WHERE ref$77.mgr = reports$159.mgr)))
//        SELECT COUNT(1) FROM recursive$157 as recref$13
//      """
//}

type Shares = (by: String, of: String, percent: Int)
type CShares = (byCom: String, ofCom: String, tot: Int)
type CompanyControlDB = (shares: Shares, emptyShares: Shares, control: (com1: String, com2: String))

given CompanyControlDBs: TestDatabase[CompanyControlDB] with
  override def tables = (
    shares = Table[Shares]("shares"),
    emptyShares = Table[Shares]("emptyShares"),
    control = Table[(com1: String, com2: String)]("emptyControl")
  )
class RecursionCompanyControlTest extends SQLStringAggregationTest[CompanyControlDB, Int] {
  def testDescription: String = "Company control"

  def query() =
    val (cshares, control) = fix(testDB.tables.emptyShares, testDB.tables.control)((cshares, control) =>
      val csharesRecur = control.flatMap(con =>
        cshares
          .filter(cs => cs.by == con.com2)
          .map(cs => (by = con.com1, of = cs.of, percent = cs.percent))
      ).distinct
      val controlRecur = cshares.filter(s => s.percent > 50).map(s => (com1 = s.by, com2 = s.of)).distinct
      (csharesRecur, controlRecur)
    )
    cshares.aggregate(c => sum(c.percent))

  def expectedQueryPattern: String =
    """
        WITH RECURSIVE
            recursive$73 AS
                ((SELECT * FROM emptyShares as emptyShares$74)
                  UNION
                 ((SELECT ref$34.com1 as by, ref$35.of as of, ref$35.percent as percent
                   FROM recursive$74 as ref$34, recursive$73 as ref$35
                   WHERE ref$35.by = ref$34.com2))),
            recursive$74 AS
                ((SELECT * FROM emptyControl as emptyControl$79)
                  UNION
                ((SELECT ref$37.by as com1, ref$37.of as com2
                 FROM recursive$73 as ref$37
                 WHERE ref$37.percent > 50)))
        SELECT SUM(recref$6.percent) FROM recursive$73 as recref$6
      """
}