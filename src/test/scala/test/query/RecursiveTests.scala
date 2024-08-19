package test.query.recursive
import test.{SQLStringQueryTest, TestDatabase}
import tyql.*
import Query.*

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
      SELECT ref$D.x as x, edges$C.y as y
      FROM recursive$A as ref$D, edges as edges$C
      WHERE ref$D.y = edges$C.x) SELECT * FROM recursive$A as recref$E
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
      SELECT ref$D.x as x, edges$C.y as y
      FROM recursive$A as ref$D, edges as edges$C
      WHERE ref$D.y = edges$C.x)
    SELECT * FROM recursive$A as recref$F
      """
}

// TODO: decide semantics, subquery, or flatten?
//class Recursion3Test extends SQLStringQueryTest[TCDB, Edge] {
//  def testDescription: String = "TC with multiple recursive cases"
//
//  def query() =
//    val path = testDB.tables.edges.unionAll(testDB.tables.edges)
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
//          UNION ALL
//        SELECT * FROM edges as edges$E
//          UNION ALL
//        SELECT recursive$A.x as x, edges$C.y as y
//        FROM recursive$A, edges as edges$C
//        WHERE recursive$A.y = edges$C.x
//          UNION ALL
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
      )
    ).map(p => p.x)

  def expectedQueryPattern: String =
    """
      WITH RECURSIVE recursive$A AS
        (SELECT * FROM edges as edges$B
          UNION ALL
        SELECT ref$D.x as x, edges$C.y as y
        FROM recursive$A as ref$D, edges as edges$C
        WHERE ref$D.y = edges$C.x) SELECT recref$E.x FROM recursive$A as recref$E
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
          SELECT ref$Z.x as x, edges$C.y as y
          FROM recursive$A as ref$Z, edges as edges$C
          WHERE ref$Z.y = edges$C.x) SELECT * FROM recursive$A as recref$X WHERE recref$X.x > 1
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
            SELECT ref$Z.x as x, edges$C.y as y
            FROM recursive$A as ref$Z, edges as edges$C
            WHERE ref$Z.y = edges$C.x) SELECT recref$X.x FROM recursive$A as recref$X WHERE recref$X.x > 1
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
//        UNION ALL
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
      val PtoA = path.filter(e => e.x == "A")
      (P, PtoA)
    )

    pathToAResult

  def expectedQueryPattern: String =
    """
      WITH RECURSIVE
          recursive$P AS
            (SELECT * FROM edges as edges$F
                UNION ALL
             SELECT ref$Z.x as x, edges$C.y as y
             FROM recursive$P as ref$Z, edges as edges$C
             WHERE ref$Z.y = edges$C.x),
          recursive$A AS
           (SELECT * FROM empty as empty$D
              UNION ALL
            SELECT * FROM recursive$P as ref$X WHERE ref$X.x = "A")
      SELECT * FROM recursive$A as recref$Q
      """
}

class RecursiveSelfJoinTest extends SQLStringQueryTest[TCDB, Edge] {
  def testDescription: String = "define 2 recursive relations with one self join"

  def query() =
    val pathBase = testDB.tables.edges
    val pathToABase = testDB.tables.emptyEdges

    val (pathResult, pathToAResult) = fix(pathBase, pathToABase)((path, pathToA) =>
      val P = path.flatMap(p =>
        path
          .filter(p2 => p.y == p2.x)
          .map(p2 => (x = p.x, y = p2.y).toRow)
      )
      val PtoA = path.filter(e => e.x == "A")
      (P, PtoA)
    )

    pathToAResult

  def expectedQueryPattern: String =
    """
        WITH RECURSIVE
            recursive$P AS
              (SELECT * FROM edges as edges$F
                  UNION ALL
               SELECT ref$Z.x as x, ref$Y.y as y
               FROM recursive$P as ref$Z, recursive$P as ref$Y
               WHERE ref$Z.y = ref$Y.x),
            recursive$A AS
             (SELECT * FROM empty as empty$D
                UNION ALL
              SELECT * FROM recursive$P as ref$Q WHERE ref$Q.x = "A")
        SELECT * FROM recursive$A as recref$S
        """
}

class RecursiveSelfJoin2Test extends SQLStringQueryTest[TCDB, Edge] {
  def testDescription: String = "define 2 recursive relations with one self join, multiple fitler"

  def query() =
    val pathBase = testDB.tables.edges
    val pathToABase = testDB.tables.emptyEdges

    val (pathResult, pathToAResult) = fix(pathBase, pathToABase)((path, pathToA) =>
      val P = path.flatMap(p =>
        path
          .filter(p2 => p.y == p2.x)
          .filter(p2 => p.x != p2.y) // ignore it makes no sense
          .map(p2 => (x = p.x, y = p2.y).toRow)
      )
      val PtoA = path.filter(e => e.x == "A")
      (P, PtoA)
    )

    pathToAResult

  def expectedQueryPattern: String =
    """
        WITH RECURSIVE
            recursive$P AS
              (SELECT * FROM edges as edges$F
                  UNION ALL
               SELECT ref$Z.x as x, ref$Y.y as y
               FROM recursive$P as ref$Z, recursive$P as ref$Y
               WHERE ref$Z.x <> ref$Y.y AND ref$Z.y = ref$Y.x),
            recursive$A AS
             (SELECT * FROM empty as empty$D
                UNION ALL
              SELECT * FROM recursive$P as ref$Q WHERE ref$Q.x = "A")
        SELECT * FROM recursive$A as recref$S
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
        .unionAll(
          // MemoryAlias(x, x) :- Assign(x, _)
          assign.map(a => (p1 = a.p1, p2 = a.p1).toRow)
        )

    val valueFlowBase =
      assign // ValueFlow(y, x) :- Assign(y, x)
        .unionAll(
          // ValueFlow(x, x) :- Assign(x, _)
          assign.map(a => (p1 = a.p1, p2 = a.p1).toRow)
        ).unionAll(
          // ValueFlow(x, x) :- Assign(_, x)
          assign.map(a => (p1 = a.p2, p2 = a.p2).toRow)
        )

    val (valueFlowFinal, valueAliasFinal, memoryAliasFinal) = fix(valueFlowBase, testDB.tables.empty, memoryAliasBase)(
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
          )
        val MA =
          // MemoryAlias(x, w) :- (Dereference(y, x), ValueAlias(y, z), Dereference(z, w))
          dereference.flatMap(d1 =>
            valueAlias.flatMap(va =>
              dereference
                .filter(d2 => d1.p1 == va.p1 && va.p2 == d2.p1)
                .map(d2 => (p1 = d1.p2, p2 = d2.p2).toRow)
            )
          )

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
          )
        (VF, MA, VA)
    )
    valueFlowFinal

  def expectedQueryPattern: String =
    """
    WITH RECURSIVE
      recursive$A AS
        (SELECT * FROM assign as assign$D
				  UNION ALL
			  SELECT assign$E.p1 as p1, assign$E.p1 as p2 FROM assign as assign$E
					UNION ALL
				SELECT assign$F.p2 as p1, assign$F.p2 as p2 FROM assign as assign$F
					UNION ALL
				SELECT assign$G.p1 as p1, ref$J.p2 as p2
				FROM assign as assign$G, recursive$C as ref$J
				WHERE assign$G.p2 = ref$J.p1
					UNION ALL
				SELECT ref$K.p1 as p1, ref$L.p2 as p2
				FROM recursive$A as ref$K, recursive$A as ref$L
				WHERE ref$K.p2 = ref$L.p1),
		  recursive$B AS
		    (SELECT * FROM empty as empty$M
					UNION ALL
				SELECT dereference$N.p2 as p1, dereference$O.p2 as p2
				FROM dereference as dereference$N, recursive$B as ref$P, dereference as dereference$O
				WHERE dereference$N.p1 = ref$P.p1 AND ref$P.p2 = dereference$O.p1),
			recursive$C AS
			  (SELECT assign$H.p2 as p1, assign$H.p2 as p2 FROM assign as assign$H
					UNION ALL
				SELECT assign$I.p1 as p1, assign$I.p1 as p2 FROM assign as assign$I
					UNION ALL
				SELECT ref$Q.p2 as p1, ref$R.p2 as p2
				FROM recursive$A as ref$Q, recursive$A as ref$R
				WHERE ref$Q.p1 = ref$R.p1
					UNION ALL
				SELECT ref$S.p2 as p1, ref$T.p2 as p2
				FROM recursive$A as ref$S, recursive$C as ref$U, recursive$A as ref$T
				WHERE ref$S.p1 = ref$U.p1 AND ref$T.p1 = ref$U.p2)
		SELECT * FROM recursive$A as recref$V
    """
}

class RecursiveCSPAComprehensionTest extends SQLStringQueryTest[CSPADB, Location] {
  def testDescription: String = "CSPA, but with comprehensions to see if nicer"

  def query() =
    val assign = testDB.tables.assign
    val dereference = testDB.tables.dereference

    val memoryAliasBase =
      // MemoryAlias(x, x) :- Assign(_, x)
      assign.map(a => (p1 = a.p2, p2 = a.p2).toRow)
        .unionAll(
          // MemoryAlias(x, x) :- Assign(x, _)
          assign.map(a => (p1 = a.p1, p2 = a.p1).toRow)
        )

    val valueFlowBase =
      assign // ValueFlow(y, x) :- Assign(y, x)
        .unionAll(
          // ValueFlow(x, x) :- Assign(x, _)
          assign.map(a => (p1 = a.p1, p2 = a.p1).toRow)
        ).unionAll(
          // ValueFlow(x, x) :- Assign(_, x)
          assign.map(a => (p1 = a.p2, p2 = a.p2).toRow)
        )

    val (valueFlowFinal, valueAliasFinal, memoryAliasFinal) = fix(valueFlowBase, testDB.tables.empty, memoryAliasBase)(
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
        val VF = vfDef1.unionAll(vfDef2)

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
        val VA = vaDef1.unionAll(vaDef2)

        (VF, MA, VA)
    )
    valueFlowFinal
  def expectedQueryPattern: String =
    """
        WITH RECURSIVE
      recursive$A AS
        (SELECT * FROM assign as assign$D
				  UNION ALL
			  SELECT assign$E.p1 as p1, assign$E.p1 as p2 FROM assign as assign$E
					UNION ALL
				SELECT assign$F.p2 as p1, assign$F.p2 as p2 FROM assign as assign$F
					UNION ALL
				SELECT assign$G.p1 as p1, ref$J.p2 as p2
				FROM assign as assign$G, recursive$C as ref$J
				WHERE assign$G.p2 = ref$J.p1
					UNION ALL
				SELECT ref$K.p1 as p1, ref$L.p2 as p2
				FROM recursive$A as ref$K, recursive$A as ref$L
				WHERE ref$K.p2 = ref$L.p1),
		  recursive$B AS
		    (SELECT * FROM empty as empty$M
					UNION ALL
				SELECT dereference$N.p2 as p1, dereference$O.p2 as p2
				FROM dereference as dereference$N, recursive$B as ref$P, dereference as dereference$O
				WHERE dereference$N.p1 = ref$P.p1 AND ref$P.p2 = dereference$O.p1),
			recursive$C AS
			  (SELECT assign$H.p2 as p1, assign$H.p2 as p2 FROM assign as assign$H
					UNION ALL
				SELECT assign$I.p1 as p1, assign$I.p1 as p2 FROM assign as assign$I
					UNION ALL
				SELECT ref$Q.p2 as p1, ref$R.p2 as p2
				FROM recursive$A as ref$Q, recursive$A as ref$R
				WHERE ref$Q.p1 = ref$R.p1
					UNION ALL
				SELECT ref$S.p2 as p1, ref$T.p2 as p2
				FROM recursive$A as ref$S, recursive$C as ref$U, recursive$A as ref$T
				WHERE ref$S.p1 = ref$U.p1 AND ref$T.p1 = ref$U.p2)
		SELECT * FROM recursive$A as recref$V
    """
}
