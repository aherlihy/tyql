package test.query.recursivebenchmarks

import test.{SQLStringAggregationTest, SQLStringQueryTest, TestDatabase}
import tyql.Query.{MultiRecursive, fix, unrestrictedBagFix, unrestrictedFix}
import tyql.Expr.{IntLit, StringLit, count, max, min, sum}
import tyql.{Ord, Table}

import language.experimental.namedTuples
import NamedTuple.*

type WeightedEdge = (src: Int, dst: Int, cost: Int)
type WeightedGraphDB = (edge: WeightedEdge, base: (dst: Int, cost: Int))

import tyql.Dialect.ansi.given
import scala.language.implicitConversions

given WeightedGraphDBs: TestDatabase[WeightedGraphDB] with
  override def tables = (
    edge = Table[WeightedEdge]("edge"),
    base = Table[(dst: Int, cost: Int)]("base")
  )

class APSPTest extends SQLStringQueryTest[WeightedGraphDB, WeightedEdge] {
  def testDescription: String = "APSP benchmark"

  def query() =
    val base = testDB.tables.edge
      .aggregate(e =>
        (src = e.src, dst = e.dst, cost = min(e.cost)).toGroupingRow
      )
      .groupBySource(e =>
        (src = e._1.src, dst = e._1.dst).toRow
      )

    val asps = base.unrestrictedFix(path =>
      path.aggregate(p =>
        path
          .filter(e =>
            p.dst == e.src
          )
          .aggregate(e =>
            (src = p.src, dst = e.dst, cost = min(p.cost + e.cost)).toGroupingRow
          )
      )
        .groupBySource(p =>
          (g1 = p._1.src, g2 = p._2.dst).toRow
        ).distinct
    )
    asps
      .aggregate(a =>
        (src = a.src, dst = a.dst, cost = min(a.cost)).toGroupingRow
      )
      .groupBySource(p =>
        (g1 = p._1.src, g2 = p._1.dst).toRow
      )

  def expectedQueryPattern: String =
    """
   WITH RECURSIVE
    recursive$1 AS
      ((SELECT edge$1.src as src, edge$1.dst as dst, MIN(edge$1.cost) as cost
        FROM edge as edge$1 GROUP BY edge$1.src, edge$1.dst)
        UNION
        ((SELECT ref$2.src as src, ref$3.dst as dst, MIN(ref$2.cost + ref$3.cost) as cost
          FROM recursive$1 as ref$2, recursive$1 as ref$3
          WHERE ref$2.dst = ref$3.src
          GROUP BY ref$2.src, ref$3.dst)))
   SELECT recref$0.src as src, recref$0.dst as dst, MIN(recref$0.cost) as cost
   FROM recursive$1 as recref$0
   GROUP BY recref$0.src, recref$0.dst
  """
}

type Edge = (x: Int, y: Int)
type GraphDB = (edge: Edge)

given GraphDBs: TestDatabase[GraphDB] with
  override def tables = (
    edge = Table[Edge]("edge"),
  )

class CCMonotoneTest extends SQLStringQueryTest[GraphDB, (x: Int, id: Int)] {
  def testDescription: String = "Connected components monotone benchmark"

  def query() =
    val undirected = testDB.tables.edge.union( // cyclic
      testDB.tables.edge.map(e =>
        (x = e.y, y = e.x).toRow
      ))
    val base = undirected.map(b => (x = b.x, y = b.x)) // self cycles

    base.unrestrictedFix(cc =>
      cc.flatMap(path1 =>
        undirected
          .filter(base => path1.y == base.x)
          .map(path2 => (x = path1.x, y = path2.y).toRow)
      ).distinct
    ).aggregate(g => (x = g.x, id = min(g.y)).toGroupingRow).groupBySource(a => a._1.x)

  def expectedQueryPattern: String =
    """
WITH RECURSIVE
  recursive$1 AS
    ((SELECT subquery$5.x as x, subquery$5.x as y
      FROM
        ((SELECT * FROM edge as edge$1)
          UNION
         (SELECT edge$3.y as x, edge$3.x as y FROM edge as edge$3)) as subquery$5)
      UNION
    ((SELECT ref$2.x as x, subquery$10.y as y
      FROM recursive$1 as ref$2,
        ((SELECT * FROM edge as edge$6)
          UNION
         (SELECT edge$8.y as x, edge$8.x as y FROM edge as edge$8)) as subquery$10
      WHERE ref$2.y = subquery$10.x)))
SELECT recref$0.x as x, MIN(recref$0.y) as id FROM recursive$1 as recref$0 GROUP BY recref$0.x
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

class OrbitsTest extends SQLStringQueryTest[PlanetaryDB, Orbits] {
  def testDescription: String = "Planetary orbits from souffle benchmark"

  def query() =
    val base = testDB.tables.base
    val orbits = base.unrestrictedBagFix(orbits =>
      orbits.flatMap(p =>
        orbits
          .filter(e => p.y == e.x)
          .map(e => (x = p.x, y = e.y).toRow)
      )
    )

    orbits match
      case MultiRecursive(_, _, orbitsRef) =>
        orbits.filter(o =>
          orbitsRef
            .flatMap(o1 =>
              orbitsRef
                .filter(o2 => o1.y == o2.x)
                .map(o2 => (x = o1.x, y = o2.y).toRow)
            )
            .filter(io => o.x == io.x && o.y == io.y)
            .nonEmpty
        )

  def expectedQueryPattern: String =
    """
        WITH RECURSIVE
          recursive$1 AS
            ((SELECT * FROM base as base$1)
              UNION ALL
            ((SELECT ref$0.x as x, ref$1.y as y
              FROM recursive$1 as ref$0, recursive$1 as ref$1
              WHERE ref$0.y = ref$1.x)))
        SELECT *
        FROM recursive$1 as recref$0
        WHERE EXISTS
          (SELECT * FROM
            (SELECT ref$4.x as x, ref$5.y as y
            FROM recursive$1 as ref$4, recursive$1 as ref$5
            WHERE ref$4.y = ref$5.x) as subquery$9
          WHERE recref$0.x = subquery$9.x AND recref$0.y = subquery$9.y)
    """

}

type AndersenPointsToDB = (addressOf: Edge, assign: Edge, load: Edge, store: Edge)

given AndersenPointsToDBs: TestDatabase[AndersenPointsToDB] with
  override def tables = (
    addressOf = Table[Edge]("addressOf"),
    assign = Table[Edge]("assign"),
    load = Table[Edge]("loadT"),
    store = Table[Edge]("store")
  )

class AndersensTest extends SQLStringQueryTest[AndersenPointsToDB, Edge] {
  def testDescription: String = "Andersens points-to"

  def query() =
    val base = testDB.tables.addressOf.map(a => (x = a.x, y = a.y).toRow)
    base.unrestrictedFix(pointsTo =>
      testDB.tables.assign.flatMap(a =>
        pointsTo.filter(p => a.y == p.x).map(p =>
          (x = a.x, y = p.y).toRow
        )
      )
        .union(testDB.tables.load.flatMap(l =>
          pointsTo.flatMap(pt1 =>
            pointsTo
              .filter(pt2 => l.y == pt1.x && pt1.y == pt2.x)
              .map(pt2 =>
                (x = l.x, y = pt2.y).toRow
              )
          )
        ))
        .union(testDB.tables.store.flatMap(s =>
          pointsTo.flatMap(pt1 =>
            pointsTo
              .filter(pt2 => s.x == pt1.x && s.y == pt2.x)
              .map(pt2 =>
                (x = pt1.y, y = pt2.y).toRow
              )
          )
        ))
    )

  def expectedQueryPattern: String =
    """
       WITH RECURSIVE
          recursive$1 AS
            ((SELECT addressOf$1.x as x, addressOf$1.y as y
              FROM addressOf as addressOf$1)
              UNION
            ((SELECT assign$3.x as x, ref$2.y as y
              FROM assign as assign$3, recursive$1 as ref$2
              WHERE assign$3.y = ref$2.x)
                UNION
             (SELECT loadT$6.x as x, ref$6.y as y
              FROM loadT as loadT$6, recursive$1 as ref$5, recursive$1 as ref$6
              WHERE loadT$6.y = ref$5.x AND ref$5.y = ref$6.x)
              UNION
             (SELECT ref$9.y as x, ref$10.y as y
              FROM store as store$11, recursive$1 as ref$9, recursive$1 as ref$10
              WHERE store$11.x = ref$9.x AND store$11.y = ref$10.x)))
      SELECT * FROM recursive$1 as recref$0
    """

}

type ProgramHeapOp = (x: String, y: String, h: String)
type ProgramOp = (x: String, y: String)
type PointsToDB =
  (newPT: ProgramOp, assign: ProgramOp, loadT: ProgramHeapOp, store: ProgramHeapOp, baseHPT: ProgramHeapOp)

given PointsToDBs: TestDatabase[PointsToDB] with
  override def tables = (
    newPT = Table[ProgramOp]("new"),
    assign = Table[ProgramOp]("assign"),
    loadT = Table[ProgramHeapOp]("loadT"),
    store = Table[ProgramHeapOp]("store"),
    baseHPT = Table[ProgramHeapOp]("baseHPT")
  )

class JavaPTTest extends SQLStringQueryTest[PointsToDB, ProgramHeapOp] {
  def testDescription: String = "Field-sensitive subset-based oop points-to"

  def query() =
    val baseVPT = testDB.tables.newPT.map(a => (x = a.x, y = a.y).toRow)
    val baseHPT = testDB.tables.baseHPT
    val pt = unrestrictedBagFix((baseVPT, baseHPT))((varPointsTo, heapPointsTo) =>
      val vpt = testDB.tables.assign.flatMap(a =>
        varPointsTo.filter(p => a.y == p.x).map(p =>
          (x = a.x, y = p.y).toRow
        )
      ).unionAll(
        testDB.tables.loadT.flatMap(l =>
          heapPointsTo.flatMap(hpt =>
            varPointsTo
              .filter(vpt => l.y == vpt.x && l.h == hpt.y && vpt.y == hpt.x)
              .map(pt2 =>
                (x = l.x, y = hpt.h).toRow
              )
          )
        )
      )
      val hpt = testDB.tables.store.flatMap(s =>
        varPointsTo.flatMap(vpt1 =>
          varPointsTo
            .filter(vpt2 => s.x == vpt1.x && s.h == vpt2.x)
            .map(vpt2 =>
              (x = vpt1.y, y = s.y, h = vpt2.y).toRow
            )
        )
      )

      (vpt, hpt)
    )
    pt._2

  def expectedQueryPattern: String =
    """
        WITH RECURSIVE
          recursive$1 AS
            ((SELECT new$2.x as x, new$2.y as y
              FROM new as new$2)
                UNION ALL
            ((SELECT assign$4.x as x, ref$2.y as y
              FROM assign as assign$4, recursive$1 as ref$2
              WHERE assign$4.y = ref$2.x)
                UNION ALL
             (SELECT loadT$7.x as x, ref$5.h as y
              FROM loadT as loadT$7, recursive$2 as ref$5, recursive$1 as ref$6
              WHERE loadT$7.y = ref$6.x AND loadT$7.h = ref$5.y AND ref$6.y = ref$5.x))),
          recursive$2 AS
            ((SELECT *
              FROM baseHPT as baseHPT$13)
                UNION ALL
            ((SELECT ref$9.y as x, store$15.y as y, ref$10.y as h
              FROM store as store$15, recursive$1 as ref$9, recursive$1 as ref$10
              WHERE store$15.x = ref$9.x AND store$15.h = ref$10.x)))
        SELECT * FROM recursive$2 as recref$0
    """
}

type Location = (p1: Int, p2: Int)
type CSPADB = (assign: Location, dereference: Location, empty: Location)

given CSPADBs: TestDatabase[CSPADB] with
  override def tables = (
    assign = Table[Location]("assign"),
    dereference = Table[Location]("dereference"),
    empty = Table[Location]("empty")
  )

class CSPAComprehensionTest extends SQLStringQueryTest[CSPADB, Location] {
  def testDescription: String = "CSPA, but with comprehensions to see if nicer"
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

type Assbl = (part: String, spart: String)
type Basic = (part: String, days: Int)

type BOMDB = (assbl: Assbl, basic: Basic)

given BOMDBs: TestDatabase[BOMDB] with
  override def tables = (
    assbl = Table[Assbl]("assbl"),
    basic = Table[Basic]("basic")
  )

class BOMTest extends SQLStringQueryTest[BOMDB, (part: String, max: Int)] {
  def testDescription: String = "BOM 'days til delivery' stratified with final aggregation"

  def query() =
    val waitFor = testDB.tables.basic
    waitFor.unrestrictedBagFix(waitFor =>
      testDB.tables.assbl.flatMap(assbl =>
        waitFor
          .filter(wf => assbl.spart == wf.part)
          .map(wf => (part = assbl.part, days = wf.days).toRow)
      )
    )
      .aggregate(wf => (part = wf.part, max = max(wf.days)).toGroupingRow)
      .groupBySource(wf => (part = wf._1.part).toRow)

  def expectedQueryPattern: String =
    """
        WITH RECURSIVE
          recursive$178 AS
            ((SELECT * FROM basic as basic$178)
              UNION ALL
              ((SELECT
                  assbl$180.part as part, ref$89.days as days
                FROM assbl as assbl$180, recursive$178 as ref$89
                WHERE assbl$180.spart = ref$89.part)))
        SELECT recref$14.part as part, MAX(recref$14.days) as max
        FROM recursive$178 as recref$14
        GROUP BY recref$14.part
      """
}

class RecursionSSSPTest extends SQLStringQueryTest[WeightedGraphDB, (dst: Int, cost: Int)] {
  def testDescription: String = "Single source shortest path"

  def query() =
    val base = testDB.tables.base
    base.fix(sp =>
      testDB.tables.edge.flatMap(edge =>
        sp
          .filter(s => s.dst == edge.src)
          .map(s => (dst = edge.dst, cost = s.cost + edge.cost).toRow)
      ).distinct
    ).aggregate(s => (dst = s.dst, cost = min(s.cost)).toGroupingRow).groupBySource(s => (dst = s._1.dst).toRow)

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
          SELECT recref$5.dst as dst, MIN(recref$5.cost) as cost FROM recursive$62 as recref$5 GROUP BY recref$5.dst
        """
}

type Organizer = (orgName: String)
type Friend = (pName: String, fName: String)
type PartyDB = (organizers: Organizer, friends: Friend, counts: (fName: String, nCount: Int))

given PartyDBs: TestDatabase[PartyDB] with
  override def tables = (
    organizers = Table[Organizer]("organizer"),
    friends = Table[Friend]("friends"),
    counts = Table[(fName: String, nCount: Int)]("counts")
  )

class PartyAttendanceTest extends SQLStringQueryTest[PartyDB, (person: String)] {
  def testDescription: String = "Mutually recursive query to find people who will attend the party"

  def query() =
    val baseAttend = testDB.tables.organizers.map(o => (person = o.orgName).toRow)
    val baseCntFriends = testDB.tables.counts

    val (finalAttend, finalCntFriends) = unrestrictedBagFix(baseAttend, baseCntFriends)((attend, cntfriends) =>
      val recurAttend = cntfriends
        .filter(cf => cf.nCount > 2)
        .map(cf => (person = cf.fName).toRow)

      val recurCntFriends = testDB.tables.friends
        .aggregate(friends =>
          attend
            .filter(att => att.person == friends.fName)
            .aggregate(att => (fName = friends.pName, nCount = count(friends.fName)).toGroupingRow)
        ).groupBySource(f => (name = f._1.pName).toRow)
      (recurAttend, recurCntFriends)
    )
    finalAttend.distinct

  def expectedQueryPattern: String =
    """
      WITH RECURSIVE
        recursive$64 AS
          ((SELECT organizer$65.orgName as person
            FROM organizer as organizer$65)
            UNION ALL
          ((SELECT ref$35.fName as person
            FROM recursive$65 as ref$35
            WHERE ref$35.nCount > 2))),
        recursive$65 AS
          ((SELECT * FROM counts as counts$69)
            UNION ALL ((SELECT friends$71.pName as fName, COUNT(friends$71.fName) as nCount
            FROM friends as friends$71, recursive$64 as ref$38
            WHERE ref$38.person = friends$71.fName
            GROUP BY friends$71.pName)))
      SELECT DISTINCT * FROM recursive$64 as recref$5
    """
}

type Shares = (byC: String, of: String, percent: Int)
type Control = (com1: String, com2: String)
type CompanyControlDB = (shares: Shares, control: Control)

given CompanyControlDBs: TestDatabase[CompanyControlDB] with
  override def tables = (
    shares = Table[Shares]("shares"),
    control = Table[Control]("control")
  )

class RecursionCompanyControlTest extends SQLStringQueryTest[CompanyControlDB, Control] {
  def testDescription: String = "Company control"

  def query() =
    val (cshares, control) = unrestrictedFix(testDB.tables.shares, testDB.tables.control)((cshares, control) =>
      val csharesRecur = control.aggregate(con =>
        cshares
          .filter(cs => cs.byC == con.com2)
          .aggregate(cs => (byC = con.com1, of = cs.of, percent = sum(cs.percent)).toGroupingRow)
      ).groupBySource((con, csh) => (byC = con.com1, of = csh.of).toRow).distinct
      val controlRecur = cshares
        .filter(s => s.percent > 50)
        .map(s => (com1 = s.byC, com2 = s.of).toRow)
        .distinct
      (csharesRecur, controlRecur)
    )
    control

  def expectedQueryPattern: String =
    """
    WITH RECURSIVE
      recursive$40 AS
          ((SELECT * FROM shares as shares$41)
            UNION
          ((SELECT ref$22.com1 as byC, ref$23.of as of, SUM(ref$23.percent) as percent
            FROM recursive$41 as ref$22, recursive$40 as ref$23
            WHERE ref$23.byC = ref$22.com2
            GROUP BY ref$22.com1, ref$23.of))),
      recursive$41 AS
          ((SELECT * FROM control as control$47)
            UNION
          ((SELECT ref$27.byC as com1, ref$27.of as com2 FROM recursive$40 as ref$27
            WHERE ref$27.percent > 50)))
    SELECT * FROM recursive$41 as recref$4
      """
}

type Path = (startNode: Int, endNode: Int, path: List[Int])

class GraphalyticsDAGTest extends SQLStringQueryTest[GraphDB, Path] {
  def testDescription: String = "Enumerate all paths from a node example"

  def query() =
    val pathBase = testDB.tables.edge
      .filter(p => p.x == 1)
      .map(e => (startNode = e.x, endNode = e.y, path = List(e.x, e.y).toExpr).toRow)

    pathBase.unrestrictedBagFix(path =>
      path.flatMap(p =>
        testDB.tables.edge
          .filter(e => e.x == p.endNode && !p.path.contains(e.y))
          .map(e =>
            (startNode = p.startNode, endNode = e.y, path = p.path.append(e.y)).toRow
          )
      )
    )
      .sort(p => p.path, Ord.ASC).sort(p => p.path.length, Ord.ASC)

  def expectedQueryPattern: String =
    """
      WITH RECURSIVE
        recursive$174 AS
          ((SELECT edge$174.x as startNode, edge$174.y as endNode, [edge$174.x, edge$174.y] as path
            FROM edge as edge$174
            WHERE edge$174.x = 1)
              UNION ALL
          ((SELECT ref$94.startNode as startNode, edge$176.y as endNode, list_append(ref$94.path, edge$176.y) as path
            FROM recursive$174 as ref$94, edge as edge$176
            WHERE edge$176.x = ref$94.endNode AND NOT list_contains(ref$94.path, edge$176.y))))
      SELECT * FROM recursive$174 as recref$15 ORDER BY length(recref$15.path) ASC, path ASC
      """
}

type Parent = (parent: String, child: String)
type AncestryDB = (parent: Parent)

given ancestryDBs: TestDatabase[AncestryDB] with
  override def tables = (
    parent = Table[Parent]("parents")
  )

class AncestryTest extends SQLStringQueryTest[AncestryDB, (name: String)] {
  def testDescription: String = "Ancestry query to calculate total number of descendants in the same generation"

  def query() =
    val base = testDB.tables.parent.filter(p => p.parent == "Alice").map(e => (name = e.child, gen = IntLit(1)).toRow)
    base.fix(gen =>
      testDB.tables.parent.flatMap(parent =>
        gen
          .filter(g => parent.parent == g.name)
          .map(g => (name = parent.child, gen = g.gen + 1).toRow)
      ).distinct
    ).filter(g => g.gen == 2).map(g => (name = g.name).toRow)

  def expectedQueryPattern: String =
    """
      WITH RECURSIVE
        recursive$162 AS
          ((SELECT parents$162.child as name, 1 as gen
            FROM parents as parents$162
            WHERE parents$162.parent = 'Alice')
            UNION
          ((SELECT parents$164.child as name, ref$86.gen + 1 as gen
            FROM parents as parents$164, recursive$162 as ref$86
            WHERE parents$164.parent = ref$86.name)))
      SELECT recref$14.name as name FROM recursive$162 as recref$14 WHERE recref$14.gen = 2
            """
}

type Number = (id: Int, value: Int)
type NumberType = (value: Int, typ: String)
type EvenOddDB = (numbers: Number)

given EvenOddDBs: TestDatabase[EvenOddDB] with
  override def tables = (
    numbers = Table[Number]("numbers")
  )

class EvenOddTest extends SQLStringQueryTest[EvenOddDB, NumberType] {
  def testDescription: String = "Mutually recursive even-odd (classic)"

  def query() =
    val evenBase =
      testDB.tables.numbers.filter(n => n.value == 0).map(n => (value = n.value, typ = StringLit("even")).toRow)
    val oddBase =
      testDB.tables.numbers.filter(n => n.value == 1).map(n => (value = n.value, typ = StringLit("odd")).toRow)

    val (even, odd) = fix((evenBase, oddBase))((even, odd) =>
      val evenResult = testDB.tables.numbers.flatMap(num =>
        odd.filter(o => num.value == o.value + 1).map(o => (value = num.value, typ = StringLit("even")))
      ).distinct
      val oddResult = testDB.tables.numbers.flatMap(num =>
        even.filter(e => num.value == e.value + 1).map(e => (value = num.value, typ = StringLit("odd")))
      ).distinct
      (evenResult, oddResult)
    )
    odd

  def expectedQueryPattern: String =
    """
      WITH RECURSIVE
        recursive$1 AS
          ((SELECT numbers$2.value as value, 'even' as typ
            FROM numbers as numbers$2
            WHERE numbers$2.value = 0)
          UNION
            ((SELECT numbers$4.value as value, 'even' as typ
              FROM numbers as numbers$4, recursive$2 as ref$5
              WHERE numbers$4.value = ref$5.value + 1))),
         recursive$2 AS
          ((SELECT numbers$8.value as value, 'odd' as typ
            FROM numbers as numbers$8
            WHERE numbers$8.value = 1)
              UNION
          ((SELECT numbers$10.value as value, 'odd' as typ
            FROM numbers as numbers$10, recursive$1 as ref$8
            WHERE numbers$10.value = ref$8.value + 1)))
        SELECT * FROM recursive$2 as recref$1
    """
}

type Term = (x: Int, y: String, z: Int)
type Lits = (x: Int, y: String)
type Vars = (x: Int, y: String)
type Abs = (x: Int, y: Int, z: Int)
type App = (x: Int, y: Int, z: Int)
type BaseData = (x: Int, y: String)
type BaseCtrl = (x: Int, y: Int)

type CBADB = (term: Term, lits: Lits, vars: Vars, abs: Abs, app: App, baseData: BaseData, baseCtrl: BaseCtrl)

given CBADBs: TestDatabase[CBADB] with
  override def tables = (
    term = Table[Term]("term"),
    lits = Table[Lits]("lits"),
    vars = Table[Vars]("vars"),
    abs = Table[Abs]("abs"),
    app = Table[App]("app"),
    baseData = Table[BaseData]("baseData"),
    baseCtrl = Table[BaseCtrl]("baseCtrl")
  )

class CBATest extends SQLStringAggregationTest[CBADB, Int] {
  def testDescription: String = "Constraint-Based Analysis benchmark"

  def query() = {
    val baseData = testDB.tables.baseData
    val dataTermBase = testDB.tables.term.flatMap(t =>
      testDB.tables.lits
        .filter(l => l.x == t.z && t.y == StringLit("Lit"))
        .map(l => (x = t.x, y = l.y).toRow)
    )

    val dataVarBase = testDB.tables.baseData

    val ctrlTermBase = testDB.tables.term.filter(t => t.y == "Abs").map(t => (x = t.x, y = t.z).toRow)

    val ctrlVarBase = testDB.tables.baseCtrl

    val (dataTerm, dataVar, ctrlTerm, ctrlVar) = unrestrictedBagFix((
      dataTermBase,
      dataVarBase,
      ctrlTermBase,
      ctrlVarBase
    ))((dataTerm, dataVar, ctrlTerm, ctrlVar) => {
      val dt1 =
        for
          t <- testDB.tables.term
          dv <- dataVar
          if t.y == "Var" && t.z == dv.x
        yield (x = t.x, y = dv.y).toRow

      val dt2 =
        for
          t <- testDB.tables.term
          dt <- dataTerm
          ct <- ctrlTerm
          abs <- testDB.tables.abs
          app <- testDB.tables.app
          if t.y == "App" && t.z == app.x && dt.x == abs.z && ct.x == app.y && ct.y == abs.x
        yield (x = t.x, y = dt.y).toRow

      val dv =
        for
          ct <- ctrlTerm
          dt <- dataTerm
          abs <- testDB.tables.abs
          app <- testDB.tables.app
          if ct.x == app.y && ct.y == abs.x && dt.x == app.z
        yield (x = abs.y, y = dt.y).toRow

      val ct1 =
        for
          t <- testDB.tables.term
          cv <- ctrlVar
          if t.y == "Var" && t.z == cv.x
        yield (x = t.x, y = cv.y).toRow
      val ct2 =
        for
          t <- testDB.tables.term
          ct1 <- ctrlTerm
          ct2 <- ctrlTerm
          abs <- testDB.tables.abs
          app <- testDB.tables.app
          if t.y == "App" && t.z == app.x && ct1.x == abs.z && ct2.x == app.y && ct2.y == abs.x
        yield (x = t.x, y = ct1.y).toRow

      val cv =
        for
          ct1 <- ctrlTerm
          ct2 <- ctrlTerm
          abs <- testDB.tables.abs
          app <- testDB.tables.app
          if ct1.x == app.y && ct1.y == abs.x && ct2.x == app.z
        yield (x = abs.y, y = ct2.y).toRow

      (dt1.unionAll(dt2), dv, ct1.unionAll(ct2), cv)
    })

    dataTerm.distinct.size
  }

  def expectedQueryPattern: String =
    """
       WITH RECURSIVE
         recursive$40 AS
            ((SELECT term$43.x as x, lits$44.y as y
              FROM term as term$43, lits as lits$44
              WHERE lits$44.x = term$43.z AND term$43.y = 'Lit')
                UNION ALL
            ((SELECT term$47.x as x, ref$28.y as y
              FROM term as term$47, recursive$41 as ref$28
              WHERE term$47.y = 'Var' AND term$47.z = ref$28.x)
                UNION ALL
             (SELECT term$50.x as x, ref$31.y as y
              FROM term as term$50, recursive$40 as ref$31, recursive$42 as ref$32, abs as abs$51, app as app$52
              WHERE term$50.y = 'App' AND term$50.z = app$52.x AND ref$31.x = abs$51.z AND ref$32.x = app$52.y AND ref$32.y = abs$51.x))),
        recursive$41 AS
          ((SELECT * FROM baseData as baseData$60)
            UNION ALL
          ((SELECT abs$62.y as x, ref$37.y as y
            FROM recursive$42 as ref$36, recursive$40 as ref$37, abs as abs$62, app as app$63
            WHERE ref$36.x = app$63.y AND ref$36.y = abs$62.x AND ref$37.x = app$63.z))),
        recursive$42 AS
          ((SELECT term$69.x as x, term$69.z as y
            FROM term as term$69 WHERE term$69.y = 'Abs')
              UNION ALL
          ((SELECT term$71.x as x, ref$42.y as y
            FROM term as term$71, recursive$43 as ref$42
            WHERE term$71.y = 'Var' AND term$71.z = ref$42.x)
              UNION ALL
           (SELECT term$74.x as x, ref$45.y as y
            FROM term as term$74, recursive$42 as ref$45, recursive$42 as ref$46, abs as abs$75, app as app$76
            WHERE term$74.y = 'App' AND term$74.z = app$76.x AND ref$45.x = abs$75.z AND ref$46.x = app$76.y AND ref$46.y = abs$75.x))),
        recursive$43 AS
          ((SELECT * FROM baseCtrl as baseCtrl$84)
            UNION ALL
           ((SELECT abs$86.y as x, ref$51.y as y
            FROM recursive$42 as ref$50, recursive$42 as ref$51, abs as abs$86, app as app$87
            WHERE ref$50.x = app$87.y AND ref$50.y = abs$86.x AND ref$51.x = app$87.z)))
    SELECT DISTINCT COUNT(1) FROM recursive$40 as recref$3
    """
}

type Friends = (person1: String, person2: String)
type TrustDB = (friends: Friends)

given TrustDBs: TestDatabase[TrustDB] with
  override def tables = (
    friends = Table[Friends]("friends")
  )

class TrustChainTest extends SQLStringQueryTest[TrustDB, (person: String, count: Int)] {
  def testDescription: String = "Mutually recursive trust chain"

  def query() = {
    val baseFriends = testDB.tables.friends

    val (trust, friends) = unrestrictedBagFix((baseFriends, baseFriends))((trust, friends) => {
      val mutualTrustResult = friends.flatMap(f =>
        trust
          .filter(mt => mt.person2 == f.person1)
          .map(mt => (person1 = mt.person1, person2 = f.person2).toRow)
      )

      val friendsResult = trust.map(mt =>
        (person1 = mt.person1, person2 = mt.person2).toRow
      )

      (mutualTrustResult, friendsResult)
    })

    trust
      .aggregate(mt => (person = mt.person2, count = count(mt.person1)).toGroupingRow)
      .groupBySource(mt => (person = mt._1.person2).toRow).sort(mt => mt.count, Ord.ASC)
  }

  def expectedQueryPattern: String =
    """
      WITH RECURSIVE
      recursive$247 AS
        ((SELECT * FROM friends as friends$248)
          UNION ALL
        ((SELECT ref$127.person1 as person1, ref$126.person2 as person2
          FROM recursive$248 as ref$126, recursive$247 as ref$127
          WHERE ref$127.person2 = ref$126.person1))),
      recursive$248 AS
        ((SELECT * FROM friends as friends$253)
          UNION ALL
        ((SELECT ref$129.person1 as person1, ref$129.person2 as person2
          FROM recursive$247 as ref$129)))
      SELECT recref$19.person2 as person, COUNT(recref$19.person1) as count
      FROM recursive$247 as recref$19
      GROUP BY recref$19.person2 ORDER BY count ASC
    """
}
//
//class MutualTrustChainTest extends SQLStringQueryTest[TrustDB, (person: String, count: Int)] {
//  def testDescription: String = "Mutually recursive trust chain, non-linear recursion"
//
//  def query() = {
//    val baseFriends = testDB.tables.friends
//
//    val (trust, friends) = unrestrictedBagFix((baseFriends, baseFriends))((trust, friends) => {
//      val mutualTrustResult = friends.flatMap(f =>
//        trust
//          .filter(mt => mt.person2 == f.person1)
//          .map(mt => (person1 = mt.person1, person2 = f.person2).toRow)
//      )
//
//      val friendsResult = trust.flatMap(t1 =>
//        trust.filter(t2 => t1.person1 == t2.person2 && t2.person2 == t2.person1).map(t1 =>
//          (person1 = t1.person1, person2 = t1.person2).toRow
//        )
//      ).union(
//        trust.flatMap(t1 =>
//          trust.filter(t2 => t1.person2 == t2.person1 && t2.person2 == t2.person1).map(t1 =>
//            (person1 = t1.person1, person2 = t1.person2).toRow
//          )
//        )
//      )
//
//      (mutualTrustResult, friendsResult)
//    })
//
//    trust
//      .aggregate(mt => (person = mt.person2, count = count(mt.person1)).toGroupingRow)
//      .groupBySource(mt => (person = mt._1.person2).toRow).sort(mt => mt.count, Ord.ASC)
//  }
//
//  def expectedQueryPattern: String =
//    """
//        WITH RECURSIVE
//        recursive$247 AS
//          ((SELECT * FROM friends as friends$248)
//            UNION ALL
//          ((SELECT ref$127.person1 as person1, ref$126.person2 as person2
//            FROM recursive$248 as ref$126, recursive$247 as ref$127
//            WHERE ref$127.person2 = ref$126.person1))),
//        recursive$248 AS
//          ((SELECT * FROM friends as friends$253)
//            UNION ALL
//          ((SELECT ref$129.person1 as person1, ref$129.person2 as person2
//            FROM recursive$247 as ref$129)))
//        SELECT recref$19.person2 as person, COUNT(recref$19.person1) as count
//        FROM recursive$247 as recref$19
//        GROUP BY recref$19.person2 ORDER BY count ASC
//      """
//}

type Instruction = (opN: String, varN: String)
type Jump = (a: String, b: String)

type FlowDB = (readOp: Instruction, writeOp: Instruction, jumpOp: Jump)
given FlowDBs: TestDatabase[FlowDB] with
  override def tables = (
    readOp = Table[Instruction]("readOp"),
    writeOp = Table[Instruction]("writeOp"),
    jumpOp = Table[Jump]("jumpOp")
  )

class FlowTest extends SQLStringQueryTest[FlowDB, (r: String, w: String)] {
  def testDescription: String = "Data flow query"

  def query() = {
    testDB.tables.jumpOp
      .unrestrictedBagFix(flow =>
        flow.flatMap(f1 =>
          flow.filter(f2 => f1.b == f2.a).map(f2 =>
            (a = f1.a, b = f2.b).toRow
          )
        )
      )
      .flatMap(f =>
        testDB.tables.readOp.flatMap(r =>
          testDB.tables.writeOp
            .filter(w => w.opN == f.a && w.varN == r.varN && f.b == r.opN)
            .map(w => (r = r.opN, w = w.opN).toRow)
        )
      )
  }

  def expectedQueryPattern: String =
    """
     WITH RECURSIVE
      recursive$161 AS
        ((SELECT * FROM jumpOp as jumpOp$161)
          UNION ALL
        ((SELECT ref$78.a as a, ref$79.b as b
          FROM recursive$161 as ref$78, recursive$161 as ref$79
          WHERE ref$78.b = ref$79.a)))
    SELECT readOp$168.opN as r, writeOp$169.opN as w
     FROM recursive$161 as recref$13, readOp as readOp$168, writeOp as writeOp$169
     WHERE writeOp$169.opN = recref$13.a AND writeOp$169.varN = readOp$168.varN AND recref$13.b = readOp$168.opN
    """
}
class PointsToCountTest extends SQLStringAggregationTest[PointsToDB, Int] {
  def testDescription: String = "PointsToCount applied to Field-sensitive subset-based oop points-to"

  def query() =
    val baseVPT = testDB.tables.newPT.map(a => (x = a.x, y = a.y).toRow)
    val baseHPT = testDB.tables.baseHPT
    val pt = unrestrictedFix((baseVPT, baseHPT))((varPointsTo, heapPointsTo) =>
      val vpt = testDB.tables.assign.flatMap(a =>
        varPointsTo.filter(p => a.y == p.x).map(p =>
          (x = a.x, y = p.y).toRow
        )
      ).union(
        testDB.tables.loadT.flatMap(l =>
          heapPointsTo.flatMap(hpt =>
            varPointsTo
              .filter(vpt => l.y == vpt.x && l.h == hpt.y && vpt.y == hpt.x)
              .map(pt2 =>
                (x = l.x, y = hpt.h).toRow
              )
          )
        )
      )
      val hpt = testDB.tables.store.flatMap(s =>
        varPointsTo.flatMap(vpt1 =>
          varPointsTo
            .filter(vpt2 => s.x == vpt1.x && s.h == vpt2.x)
            .map(vpt2 =>
              (x = vpt1.y, y = s.y, h = vpt2.y).toRow
            )
        )
      )

      (vpt, hpt)
    )
    pt._1.filter(vpt => vpt.x == "r").size

  def expectedQueryPattern: String =
    """
        WITH RECURSIVE
          recursive$1 AS
            ((SELECT new$2.x as x, new$2.y as y
              FROM new as new$2)
                UNION
            ((SELECT assign$4.x as x, ref$2.y as y
              FROM assign as assign$4, recursive$1 as ref$2
              WHERE assign$4.y = ref$2.x)
                UNION
             (SELECT loadT$7.x as x, ref$5.h as y
              FROM loadT as loadT$7, recursive$2 as ref$5, recursive$1 as ref$6
              WHERE loadT$7.y = ref$6.x AND loadT$7.h = ref$5.y AND ref$6.y = ref$5.x))),
          recursive$2 AS
            ((SELECT *
              FROM baseHPT as baseHPT$13)
                UNION
            ((SELECT ref$9.y as x, store$15.y as y, ref$10.y as h
              FROM store as store$15, recursive$1 as ref$9, recursive$1 as ref$10
              WHERE store$15.x = ref$9.x AND store$15.h = ref$10.x)))
        SELECT COUNT(1) FROM recursive$1 as recref$0 WHERE recref$0.x = 'r'
    """
}
