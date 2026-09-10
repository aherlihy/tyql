package test.query.backends

import test.query.recursivebenchmarks.{given, *}
import tyql.Query.{MultiRecursive, unrestrictedBagFix, unrestrictedFix}
import tyql.Expr.{IntLit, StringLit, count, max, min, sum}
import tyql.Ord

import language.experimental.namedTuples
import NamedTuple.*

/**
 * The sixteen Recursive Query Benchmark queries, one class per row of the
 * coverage table, each checked against the SQL TyQL generates on DuckDB,
 * PostgreSQL, SQLite and MariaDB.
 *
 * The queries are the ones the `TimeoutQueryBenchmark` benchmarks run, expressed
 * with the *unrestricted* `fix` so that every query -- including the ones TyQL's
 * type system is designed to reject -- can be built and rendered.
 *
 * Table definitions are shared with `test.query.recursivebenchmarks`.
 */

// ---------------------------------------------------------------- Even-Odd

class EvenOddBackendTest extends RQBBackendTest:
  def testDescription: String = "RQB evenodd"

  def query() =
    val evenBase = EvenOddDBs.tables.numbers.filter(n => n.value == 0)
      .map(n => (value = n.value, typ = StringLit("even")).toRow)
    val oddBase = EvenOddDBs.tables.numbers.filter(n => n.value == 1)
      .map(n => (value = n.value, typ = StringLit("odd")).toRow)

    val (even, odd) = unrestrictedFix((evenBase, oddBase))((even, odd) =>
      val evenResult = EvenOddDBs.tables.numbers.flatMap(num =>
        odd.filter(o => num.value == o.value + 1).map(o => (value = num.value, typ = StringLit("even")))
      ).distinct
      val oddResult = EvenOddDBs.tables.numbers.flatMap(num =>
        even.filter(e => num.value == e.value + 1).map(e => (value = num.value, typ = StringLit("odd")))
      ).distinct
      (evenResult, oddResult)
    )
    odd

  def duckdb: String =
    """
    WITH RECURSIVE recursive$1 AS
        ((SELECT numbers$1.value as value, 'even' as typ
        FROM numbers as numbers$1
        WHERE numbers$1.value = 0)
      UNION
          ((SELECT numbers$2.value as value, 'even' as typ
          FROM numbers as numbers$2, recursive$2 as ref$1
          WHERE numbers$2.value = ref$1.value + 1))), recursive$2 AS
        ((SELECT numbers$3.value as value, 'odd' as typ
        FROM numbers as numbers$3
        WHERE numbers$3.value = 1)
      UNION
          ((SELECT numbers$4.value as value, 'odd' as typ
          FROM numbers as numbers$4, recursive$1 as ref$2
          WHERE numbers$4.value = ref$2.value + 1)))
    SELECT *
    FROM recursive$2 as recref$1
    """

  override def sqlite: String =
    """
    WITH RECURSIVE recursive$1 AS
      (SELECT numbers$1.value as value, 'even' as typ
      FROM numbers as numbers$1
      WHERE numbers$1.value = 0
      UNION
      SELECT numbers$2.value as value, 'even' as typ
      FROM numbers as numbers$2, recursive$2 as ref$1
      WHERE numbers$2.value = ref$1.value + 1), recursive$2 AS
      (SELECT numbers$3.value as value, 'odd' as typ
      FROM numbers as numbers$3
      WHERE numbers$3.value = 1
      UNION
      SELECT numbers$4.value as value, 'odd' as typ
      FROM numbers as numbers$4, recursive$1 as ref$2
      WHERE numbers$4.value = ref$2.value + 1)
    SELECT *
    FROM recursive$2 as recref$1
    """

// -------------------------------------------------------------------- CSPA

class CSPABackendTest extends RQBBackendTest:
  def testDescription: String = "RQB cspa"

  def query() =
    val assign = CSPADBs.tables.assign
    val dereference = CSPADBs.tables.dereference

    val memoryAliasBase =
      assign.map(a => (p1 = a.p2, p2 = a.p2).toRow)
        .union(assign.map(a => (p1 = a.p1, p2 = a.p1).toRow))

    val valueFlowBase =
      assign
        .union(assign.map(a => (p1 = a.p1, p2 = a.p1).toRow))
        .union(assign.map(a => (p1 = a.p2, p2 = a.p2).toRow))

    val (valueFlowFinal, valueAliasFinal, memoryAliasFinal) =
      unrestrictedFix((valueFlowBase, CSPADBs.tables.empty, memoryAliasBase))(
        (valueFlow, valueAlias, memoryAlias) =>
          val vfDef1 =
            for
              a <- assign
              m <- memoryAlias
              if a.p2 == m.p1
            yield (p1 = a.p1, p2 = m.p2).toRow
          val vfDef2 =
            for
              vf1 <- valueFlow
              vf2 <- valueFlow
              if vf1.p2 == vf2.p1
            yield (p1 = vf1.p1, p2 = vf2.p2).toRow
          val VF = vfDef1.union(vfDef2)

          val MA =
            for
              d1 <- dereference
              va <- valueAlias
              d2 <- dereference
              if d1.p1 == va.p1 && va.p2 == d2.p1
            yield (p1 = d1.p2, p2 = d2.p2).toRow

          val vaDef1 =
            for
              vf1 <- valueFlow
              vf2 <- valueFlow
              if vf1.p1 == vf2.p1
            yield (p1 = vf1.p2, p2 = vf2.p2).toRow
          val vaDef2 =
            for
              vf1 <- valueFlow
              m <- memoryAlias
              vf2 <- valueFlow
              if vf1.p1 == m.p1 && vf2.p1 == m.p2
            yield (p1 = vf1.p2, p2 = vf2.p2).toRow
          val VA = vaDef1.union(vaDef2)

          (VF, VA, MA.distinct)
      )
    valueFlowFinal

  // PostgreSQL: no reference variant (Postgres rejects the query for mutual recursion), but
  // recursive$1 is non-linear so the inner-CTE workaround applies.
  def duckdb: String =
    """
    WITH RECURSIVE recursive$1 AS
        ((SELECT *
        FROM assign as assign$1)
      UNION
          ((SELECT assign$2.p1 as p1, assign$2.p1 as p2
          FROM assign as assign$2)
        UNION
          (SELECT assign$3.p2 as p1, assign$3.p2 as p2
          FROM assign as assign$3)
        UNION
          (SELECT assign$4.p1 as p1, ref$1.p2 as p2
          FROM assign as assign$4, recursive$2 as ref$1
          WHERE assign$4.p2 = ref$1.p1)
        UNION
          (SELECT ref$2.p1 as p1, ref$3.p2 as p2
          FROM recursive$1 as ref$2, recursive$1 as ref$3
          WHERE ref$2.p2 = ref$3.p1))), recursive$3 AS
        ((SELECT *
        FROM empty as empty$1)
      UNION
          ((SELECT ref$4.p2 as p1, ref$5.p2 as p2
          FROM recursive$1 as ref$4, recursive$1 as ref$5
          WHERE ref$4.p1 = ref$5.p1)
        UNION
          (SELECT ref$6.p2 as p1, ref$7.p2 as p2
          FROM recursive$1 as ref$6, recursive$2 as ref$8, recursive$1 as ref$7
          WHERE ref$6.p1 = ref$8.p1 AND ref$7.p1 = ref$8.p2))), recursive$2 AS
        ((SELECT assign$5.p2 as p1, assign$5.p2 as p2
        FROM assign as assign$5)
      UNION
          ((SELECT assign$6.p1 as p1, assign$6.p1 as p2
          FROM assign as assign$6)
        UNION
          (SELECT dereference$1.p2 as p1, dereference$2.p2 as p2
          FROM dereference as dereference$1, recursive$3 as ref$9, dereference as dereference$2
          WHERE dereference$1.p1 = ref$9.p1 AND ref$9.p2 = dereference$2.p1)))
    SELECT *
    FROM recursive$1 as recref$1
    """

  override def postgres: String =
    """
    WITH RECURSIVE recursive$1 AS
        ((SELECT *
        FROM assign as assign$1)
      UNION
        (WITH recursive$1 AS
          (SELECT *
          FROM recursive$1)
          (SELECT assign$2.p1 as p1, assign$2.p1 as p2
          FROM assign as assign$2)
        UNION
          (SELECT assign$3.p2 as p1, assign$3.p2 as p2
          FROM assign as assign$3)
        UNION
          (SELECT assign$4.p1 as p1, ref$1.p2 as p2
          FROM assign as assign$4, recursive$2 as ref$1
          WHERE assign$4.p2 = ref$1.p1)
        UNION
          (SELECT ref$2.p1 as p1, ref$3.p2 as p2
          FROM recursive$1 as ref$2, recursive$1 as ref$3
          WHERE ref$2.p2 = ref$3.p1))), recursive$3 AS
        ((SELECT *
        FROM empty as empty$1)
      UNION
          ((SELECT ref$4.p2 as p1, ref$5.p2 as p2
          FROM recursive$1 as ref$4, recursive$1 as ref$5
          WHERE ref$4.p1 = ref$5.p1)
        UNION
          (SELECT ref$6.p2 as p1, ref$7.p2 as p2
          FROM recursive$1 as ref$6, recursive$2 as ref$8, recursive$1 as ref$7
          WHERE ref$6.p1 = ref$8.p1 AND ref$7.p1 = ref$8.p2))), recursive$2 AS
        ((SELECT assign$5.p2 as p1, assign$5.p2 as p2
        FROM assign as assign$5)
      UNION
          ((SELECT assign$6.p1 as p1, assign$6.p1 as p2
          FROM assign as assign$6)
        UNION
          (SELECT dereference$1.p2 as p1, dereference$2.p2 as p2
          FROM dereference as dereference$1, recursive$3 as ref$9, dereference as dereference$2
          WHERE dereference$1.p1 = ref$9.p1 AND ref$9.p2 = dereference$2.p1)))
    SELECT *
    FROM recursive$1 as recref$1
    """

  override def sqlite: String =
    """
    WITH RECURSIVE recursive$1 AS
      (SELECT *
      FROM assign as assign$1
      UNION
      SELECT assign$2.p1 as p1, assign$2.p1 as p2
      FROM assign as assign$2
      UNION
      SELECT assign$3.p2 as p1, assign$3.p2 as p2
      FROM assign as assign$3
      UNION
      SELECT assign$4.p1 as p1, ref$1.p2 as p2
      FROM assign as assign$4, recursive$2 as ref$1
      WHERE assign$4.p2 = ref$1.p1
      UNION
      SELECT ref$2.p1 as p1, ref$3.p2 as p2
      FROM recursive$1 as ref$2, recursive$1 as ref$3
      WHERE ref$2.p2 = ref$3.p1), recursive$3 AS
      (SELECT *
      FROM empty as empty$1
      UNION
      SELECT ref$4.p2 as p1, ref$5.p2 as p2
      FROM recursive$1 as ref$4, recursive$1 as ref$5
      WHERE ref$4.p1 = ref$5.p1
      UNION
      SELECT ref$6.p2 as p1, ref$7.p2 as p2
      FROM recursive$1 as ref$6, recursive$2 as ref$8, recursive$1 as ref$7
      WHERE ref$6.p1 = ref$8.p1 AND ref$7.p1 = ref$8.p2), recursive$2 AS
      (SELECT assign$5.p2 as p1, assign$5.p2 as p2
      FROM assign as assign$5
      UNION
      SELECT assign$6.p1 as p1, assign$6.p1 as p2
      FROM assign as assign$6
      UNION
      SELECT dereference$1.p2 as p1, dereference$2.p2 as p2
      FROM dereference as dereference$1, recursive$3 as ref$9, dereference as dereference$2
      WHERE dereference$1.p1 = ref$9.p1 AND ref$9.p2 = dereference$2.p1)
    SELECT *
    FROM recursive$1 as recref$1
    """

// ---------------------------------------------------------- Company Control

class CompanyControlBackendTest extends RQBBackendTest:
  def testDescription: String = "RQB cc"

  def query() =
    val (cshares, control) =
      unrestrictedFix((CompanyControlDBs.tables.shares, CompanyControlDBs.tables.control))((cshares, control) =>
        val csharesRecur = control.aggregate(con =>
            cshares
              .filter(cs => cs.byC == con.com2)
              .aggregate(cs => (byC = con.com1, of = cs.of, percent = sum(cs.percent)).toGroupingRow)
          ).groupBySource(
            (con, csh) => (byC = con.com1, of = csh.of).toRow
          ).distinct
        val controlRecur = cshares
          .filter(s => s.percent > 50)
          .map(s => (com1 = s.byC, com2 = s.of).toRow)
          .distinct
        (csharesRecur, controlRecur)
      )
    control

  def duckdb: String =
    """
    WITH RECURSIVE recursive$1 AS
        ((SELECT *
        FROM shares as shares$1)
      UNION
          ((SELECT ref$1.com1 as byC, ref$2.of as of, SUM(ref$2.percent) as percent
          FROM recursive$2 as ref$1, recursive$1 as ref$2
          WHERE ref$2.byC = ref$1.com2
          GROUP BY ref$1.com1, ref$2.of))), recursive$2 AS
        ((SELECT *
        FROM control as control$1)
      UNION
          ((SELECT ref$3.byC as com1, ref$3.of as com2
          FROM recursive$1 as ref$3
          WHERE ref$3.percent > 50)))
    SELECT *
    FROM recursive$2 as recref$1
    """

  override def sqlite: String =
    """
    WITH RECURSIVE recursive$1 AS
      (SELECT *
      FROM shares as shares$1
      UNION
      SELECT ref$1.com1 as byC, ref$2.of as of, SUM(ref$2.percent) as percent
      FROM recursive$2 as ref$1, recursive$1 as ref$2
      WHERE ref$2.byC = ref$1.com2
      GROUP BY ref$1.com1, ref$2.of), recursive$2 AS
      (SELECT *
      FROM control as control$1
      UNION
      SELECT ref$3.byC as com1, ref$3.of as com2
      FROM recursive$1 as ref$3
      WHERE ref$3.percent > 50)
    SELECT *
    FROM recursive$2 as recref$1
    """

// ----------------------------------------------------------- PointsToCount

class PointsToCountBackendTest extends RQBBackendTest:
  def testDescription: String = "RQB pointstocount"

  def query() =
    val baseVPT = PointsToDBs.tables.newPT.map(a => (x = a.x, y = a.y).toRow)
    val baseHPT = PointsToDBs.tables.baseHPT
    val pt = unrestrictedFix((baseVPT, baseHPT))((varPointsTo, heapPointsTo) =>
      val vpt = PointsToDBs.tables.assign.flatMap(a =>
        varPointsTo.filter(p => a.y == p.x).map(p => (x = a.x, y = p.y).toRow)
      ).union(
        PointsToDBs.tables.loadT.flatMap(l =>
          heapPointsTo.flatMap(hpt =>
            varPointsTo
              .filter(vpt => l.y == vpt.x && l.h == hpt.y && vpt.y == hpt.x)
              .map(pt2 => (x = l.x, y = hpt.h).toRow)
          )
        )
      )
      val hpt = PointsToDBs.tables.store.flatMap(s =>
        varPointsTo.flatMap(vpt1 =>
          varPointsTo
            .filter(vpt2 => s.x == vpt1.x && s.h == vpt2.x)
            .map(vpt2 => (x = vpt1.y, y = s.y, h = vpt2.y).toRow)
        )
      )
      (vpt, hpt)
    )
    pt._1.filter(vpt => vpt.x == "r").size

  // PostgreSQL: no reference variant (Postgres rejects the query for mutual recursion), but both
  // blocks are non-linear so the inner-CTE workaround applies.
  def duckdb: String =
    """
    WITH RECURSIVE recursive$1 AS
        ((SELECT new$1.x as x, new$1.y as y
        FROM new as new$1)
      UNION
          ((SELECT assign$1.x as x, ref$1.y as y
          FROM assign as assign$1, recursive$1 as ref$1
          WHERE assign$1.y = ref$1.x)
        UNION
          (SELECT loadT$1.x as x, ref$2.h as y
          FROM loadT as loadT$1, recursive$2 as ref$2, recursive$1 as ref$3
          WHERE loadT$1.y = ref$3.x AND loadT$1.h = ref$2.y AND ref$3.y = ref$2.x))), recursive$2 AS
        ((SELECT *
        FROM baseHPT as baseHPT$1)
      UNION
          ((SELECT ref$4.y as x, store$1.y as y, ref$5.y as h
          FROM store as store$1, recursive$1 as ref$4, recursive$1 as ref$5
          WHERE store$1.x = ref$4.x AND store$1.h = ref$5.x)))
    SELECT COUNT(1)
    FROM recursive$1 as recref$1
    WHERE recref$1.x = 'r'
    """

  override def postgres: String =
    """
    WITH RECURSIVE recursive$1 AS
        ((SELECT new$1.x as x, new$1.y as y
        FROM new as new$1)
      UNION
        (WITH recursive$1 AS
          (SELECT *
          FROM recursive$1)
          (SELECT assign$1.x as x, ref$1.y as y
          FROM assign as assign$1, recursive$1 as ref$1
          WHERE assign$1.y = ref$1.x)
        UNION
          (SELECT loadT$1.x as x, ref$2.h as y
          FROM loadT as loadT$1, recursive$2 as ref$2, recursive$1 as ref$3
          WHERE loadT$1.y = ref$3.x AND loadT$1.h = ref$2.y AND ref$3.y = ref$2.x))), recursive$2 AS
        ((SELECT *
        FROM baseHPT as baseHPT$1)
      UNION
          ((SELECT ref$4.y as x, store$1.y as y, ref$5.y as h
          FROM store as store$1, recursive$1 as ref$4, recursive$1 as ref$5
          WHERE store$1.x = ref$4.x AND store$1.h = ref$5.x)))
    SELECT COUNT(1)
    FROM recursive$1 as recref$1
    WHERE recref$1.x = 'r'
    """

  override def sqlite: String =
    """
    WITH RECURSIVE recursive$1 AS
      (SELECT new$1.x as x, new$1.y as y
      FROM new as new$1
      UNION
      SELECT assign$1.x as x, ref$1.y as y
      FROM assign as assign$1, recursive$1 as ref$1
      WHERE assign$1.y = ref$1.x
      UNION
      SELECT loadT$1.x as x, ref$2.h as y
      FROM loadT as loadT$1, recursive$2 as ref$2, recursive$1 as ref$3
      WHERE loadT$1.y = ref$3.x AND loadT$1.h = ref$2.y AND ref$3.y = ref$2.x), recursive$2 AS
      (SELECT *
      FROM baseHPT as baseHPT$1
      UNION
      SELECT ref$4.y as x, store$1.y as y, ref$5.y as h
      FROM store as store$1, recursive$1 as ref$4, recursive$1 as ref$5
      WHERE store$1.x = ref$4.x AND store$1.h = ref$5.x)
    SELECT COUNT(1)
    FROM recursive$1 as recref$1
    WHERE recref$1.x = 'r'
    """

// ------------------------------------------------------------ Chain Of Trust

class TrustChainBackendTest extends RQBBackendTest:
  def testDescription: String = "RQB trustchain"

  def query() =
    val baseFriends = TrustDBs.tables.friends

    val (trust, friends) = unrestrictedBagFix((baseFriends, baseFriends))((trust, friends) =>
      val mutualTrustResult = friends.flatMap(f =>
        trust
          .filter(mt => mt.person2 == f.person1)
          .map(mt => (person1 = mt.person1, person2 = f.person2).toRow)
      )
      val friendsResult = trust.map(mt => (person1 = mt.person1, person2 = mt.person2).toRow)
      (mutualTrustResult, friendsResult)
    )

    trust
      .aggregate(mt => (person = mt.person2, count = count(mt.person1)).toGroupingRow)
      .groupBySource(mt => (person = mt._1.person2).toRow).sort(mt => mt.count, Ord.ASC)

  def duckdb: String =
    """
    WITH RECURSIVE recursive$1 AS
        ((SELECT *
        FROM friends as friends$1)
      UNION ALL
          ((SELECT ref$1.person1 as person1, ref$2.person2 as person2
          FROM recursive$2 as ref$2, recursive$1 as ref$1
          WHERE ref$1.person2 = ref$2.person1))), recursive$2 AS
        ((SELECT *
        FROM friends as friends$2)
      UNION ALL
          ((SELECT ref$3.person1 as person1, ref$3.person2 as person2
          FROM recursive$1 as ref$3)))
    SELECT recref$1.person2 as person, COUNT(recref$1.person1) as count
    FROM recursive$1 as recref$1
    GROUP BY recref$1.person2
    ORDER BY count ASC
    """

  override def sqlite: String =
    """
    WITH RECURSIVE recursive$1 AS
      (SELECT *
      FROM friends as friends$1
      UNION ALL
      SELECT ref$1.person1 as person1, ref$2.person2 as person2
      FROM recursive$2 as ref$2, recursive$1 as ref$1
      WHERE ref$1.person2 = ref$2.person1), recursive$2 AS
      (SELECT *
      FROM friends as friends$2
      UNION ALL
      SELECT ref$3.person1 as person1, ref$3.person2 as person2
      FROM recursive$1 as ref$3)
    SELECT recref$1.person2 as person, COUNT(recref$1.person1) as count
    FROM recursive$1 as recref$1
    GROUP BY recref$1.person2
    ORDER BY count ASC
    """

// ---------------------------------------------------------- Java Points-To

class JavaPointsToBackendTest extends RQBBackendTest:
  def testDescription: String = "RQB javapointsto"

  def query() =
    val baseVPT = PointsToDBs.tables.newPT.map(a => (x = a.x, y = a.y).toRow)
    val baseHPT = PointsToDBs.tables.baseHPT
    val pt = unrestrictedBagFix((baseVPT, baseHPT))((varPointsTo, heapPointsTo) =>
      val vpt = PointsToDBs.tables.assign.flatMap(a =>
        varPointsTo.filter(p => a.y == p.x).map(p => (x = a.x, y = p.y).toRow)
      ).unionAll(
        PointsToDBs.tables.loadT.flatMap(l =>
          heapPointsTo.flatMap(hpt =>
            varPointsTo
              .filter(vpt => l.y == vpt.x && l.h == hpt.y && vpt.y == hpt.x)
              .map(pt2 => (x = l.x, y = hpt.h).toRow)
          )
        )
      )
      val hpt = PointsToDBs.tables.store.flatMap(s =>
        varPointsTo.flatMap(vpt1 =>
          varPointsTo
            .filter(vpt2 => s.x == vpt1.x && s.h == vpt2.x)
            .map(vpt2 => (x = vpt1.y, y = s.y, h = vpt2.y).toRow)
        )
      )
      (vpt, hpt)
    )
    pt._2

  // PostgreSQL: no reference variant (Postgres rejects the query for mutual recursion), but both
  // blocks are non-linear so the inner-CTE workaround applies.
  def duckdb: String =
    """
    WITH RECURSIVE recursive$1 AS
        ((SELECT new$1.x as x, new$1.y as y
        FROM new as new$1)
      UNION ALL
          ((SELECT assign$1.x as x, ref$1.y as y
          FROM assign as assign$1, recursive$1 as ref$1
          WHERE assign$1.y = ref$1.x)
        UNION ALL
          (SELECT loadT$1.x as x, ref$2.h as y
          FROM loadT as loadT$1, recursive$2 as ref$2, recursive$1 as ref$3
          WHERE loadT$1.y = ref$3.x AND loadT$1.h = ref$2.y AND ref$3.y = ref$2.x))), recursive$2 AS
        ((SELECT *
        FROM baseHPT as baseHPT$1)
      UNION ALL
          ((SELECT ref$4.y as x, store$1.y as y, ref$5.y as h
          FROM store as store$1, recursive$1 as ref$4, recursive$1 as ref$5
          WHERE store$1.x = ref$4.x AND store$1.h = ref$5.x)))
    SELECT *
    FROM recursive$2 as recref$1
    """

  override def postgres: String =
    """
    WITH RECURSIVE recursive$1 AS
        ((SELECT new$1.x as x, new$1.y as y
        FROM new as new$1)
      UNION ALL
        (WITH recursive$1 AS
          (SELECT *
          FROM recursive$1)
          (SELECT assign$1.x as x, ref$1.y as y
          FROM assign as assign$1, recursive$1 as ref$1
          WHERE assign$1.y = ref$1.x)
        UNION ALL
          (SELECT loadT$1.x as x, ref$2.h as y
          FROM loadT as loadT$1, recursive$2 as ref$2, recursive$1 as ref$3
          WHERE loadT$1.y = ref$3.x AND loadT$1.h = ref$2.y AND ref$3.y = ref$2.x))), recursive$2 AS
        ((SELECT *
        FROM baseHPT as baseHPT$1)
      UNION ALL
          ((SELECT ref$4.y as x, store$1.y as y, ref$5.y as h
          FROM store as store$1, recursive$1 as ref$4, recursive$1 as ref$5
          WHERE store$1.x = ref$4.x AND store$1.h = ref$5.x)))
    SELECT *
    FROM recursive$2 as recref$1
    """

  override def sqlite: String =
    """
    WITH RECURSIVE recursive$1 AS
      (SELECT new$1.x as x, new$1.y as y
      FROM new as new$1
      UNION ALL
      SELECT assign$1.x as x, ref$1.y as y
      FROM assign as assign$1, recursive$1 as ref$1
      WHERE assign$1.y = ref$1.x
      UNION ALL
      SELECT loadT$1.x as x, ref$2.h as y
      FROM loadT as loadT$1, recursive$2 as ref$2, recursive$1 as ref$3
      WHERE loadT$1.y = ref$3.x AND loadT$1.h = ref$2.y AND ref$3.y = ref$2.x), recursive$2 AS
      (SELECT *
      FROM baseHPT as baseHPT$1
      UNION ALL
      SELECT ref$4.y as x, store$1.y as y, ref$5.y as h
      FROM store as store$1, recursive$1 as ref$4, recursive$1 as ref$5
      WHERE store$1.x = ref$4.x AND store$1.h = ref$5.x)
    SELECT *
    FROM recursive$2 as recref$1
    """

// ------------------------------------------------------------------- Party

class PartyBackendTest extends RQBBackendTest:
  def testDescription: String = "RQB party"

  def query() =
    val baseAttend = PartyDBs.tables.organizers.map(o => (person = o.orgName).toRow)
    val baseCntFriends = PartyDBs.tables.counts

    val (finalAttend, finalCntFriends) =
      unrestrictedBagFix((baseAttend, baseCntFriends))((attend, cntfriends) =>
        val recurAttend = cntfriends
          .filter(cf => cf.nCount > 2)
          .map(cf => (person = cf.fName).toRow)

        val recurCntFriends = PartyDBs.tables.friends
          .aggregate(friends =>
            attend
              .filter(att => att.person == friends.fName)
              .aggregate(att => (fName = friends.pName, nCount = count(friends.fName)).toGroupingRow)
          ).groupBySource(f => (name = f._1.pName).toRow)
        (recurAttend, recurCntFriends)
      )
    finalAttend.distinct

  def duckdb: String =
    """
    WITH RECURSIVE recursive$1 AS
        ((SELECT organizer$1.orgName as person
        FROM organizer as organizer$1)
      UNION ALL
          ((SELECT ref$1.fName as person
          FROM recursive$2 as ref$1
          WHERE ref$1.nCount > 2))), recursive$2 AS
        ((SELECT *
        FROM counts as counts$1)
      UNION ALL
          ((SELECT friends$1.pName as fName, COUNT(friends$1.fName) as nCount
          FROM friends as friends$1, recursive$1 as ref$2
          WHERE ref$2.person = friends$1.fName
          GROUP BY friends$1.pName)))
    SELECT DISTINCT *
    FROM recursive$1 as recref$1
    """

  override def sqlite: String =
    """
    WITH RECURSIVE recursive$1 AS
      (SELECT organizer$1.orgName as person
      FROM organizer as organizer$1
      UNION ALL
      SELECT ref$1.fName as person
      FROM recursive$2 as ref$1
      WHERE ref$1.nCount > 2), recursive$2 AS
      (SELECT *
      FROM counts as counts$1
      UNION ALL
      SELECT friends$1.pName as fName, COUNT(friends$1.fName) as nCount
      FROM friends as friends$1, recursive$1 as ref$2
      WHERE ref$2.person = friends$1.fName
      GROUP BY friends$1.pName)
    SELECT DISTINCT *
    FROM recursive$1 as recref$1
    """

// --------------------------------------------------------------------- CBA

class CBABackendTest extends RQBBackendTest:
  def testDescription: String = "RQB cba"

  def query() =
    val dataTermBase = CBADBs.tables.term.flatMap(t =>
      CBADBs.tables.lits
        .filter(l => l.x == t.z && t.y == StringLit("Lit"))
        .map(l => (x = t.x, y = l.y).toRow))
    val dataVarBase = CBADBs.tables.baseData
    val ctrlTermBase = CBADBs.tables.term.filter(t => t.y == "Abs").map(t => (x = t.x, y = t.z).toRow)
    val ctrlVarBase = CBADBs.tables.baseCtrl

    val (dataTerm, dataVar, ctrlTerm, ctrlVar) =
      unrestrictedBagFix((dataTermBase, dataVarBase, ctrlTermBase, ctrlVarBase))(
        (dataTerm, dataVar, ctrlTerm, ctrlVar) =>
          val dt1 =
            for
              t <- CBADBs.tables.term
              dv <- dataVar
              if t.y == "Var" && t.z == dv.x
            yield (x = t.x, y = dv.y).toRow
          val dt2 =
            for
              t <- CBADBs.tables.term
              dt <- dataTerm
              ct <- ctrlTerm
              abs <- CBADBs.tables.abs
              app <- CBADBs.tables.app
              if t.y == "App" && t.z == app.x && dt.x == abs.z && ct.x == app.y && ct.y == abs.x
            yield (x = t.x, y = dt.y).toRow
          val dv =
            for
              ct <- ctrlTerm
              dt <- dataTerm
              abs <- CBADBs.tables.abs
              app <- CBADBs.tables.app
              if ct.x == app.y && ct.y == abs.x && dt.x == app.z
            yield (x = abs.y, y = dt.y).toRow
          val ct1 =
            for
              t <- CBADBs.tables.term
              cv <- ctrlVar
              if t.y == "Var" && t.z == cv.x
            yield (x = t.x, y = cv.y).toRow
          val ct2 =
            for
              t <- CBADBs.tables.term
              ct1 <- ctrlTerm
              ct2 <- ctrlTerm
              abs <- CBADBs.tables.abs
              app <- CBADBs.tables.app
              if t.y == "App" && t.z == app.x && ct1.x == abs.z && ct2.x == app.y && ct2.y == abs.x
            yield (x = t.x, y = ct1.y).toRow
          val cv =
            for
              ct1 <- ctrlTerm
              ct2 <- ctrlTerm
              abs <- CBADBs.tables.abs
              app <- CBADBs.tables.app
              if ct1.x == app.y && ct1.y == abs.x && ct2.x == app.z
            yield (x = abs.y, y = ct2.y).toRow

          (dt1.unionAll(dt2), dv, ct1.unionAll(ct2), cv)
      )

    dataTerm.distinct.size

  // PostgreSQL: no reference variant (Postgres rejects the query for mutual recursion), but two of
  // the four blocks are non-linear so the inner-CTE workaround applies.
  def duckdb: String =
    """
    WITH RECURSIVE recursive$1 AS
        ((SELECT term$1.x as x, lits$1.y as y
        FROM term as term$1, lits as lits$1
        WHERE lits$1.x = term$1.z AND term$1.y = 'Lit')
      UNION ALL
          ((SELECT term$2.x as x, ref$1.y as y
          FROM term as term$2, recursive$2 as ref$1
          WHERE term$2.y = 'Var' AND term$2.z = ref$1.x)
        UNION ALL
          (SELECT term$3.x as x, ref$2.y as y
          FROM term as term$3, recursive$1 as ref$2, recursive$3 as ref$3, abs as abs$1, app as app$1
          WHERE term$3.y = 'App' AND term$3.z = app$1.x AND ref$2.x = abs$1.z AND ref$3.x = app$1.y AND ref$3.y = abs$1.x))), recursive$2 AS
        ((SELECT *
        FROM baseData as baseData$1)
      UNION ALL
          ((SELECT abs$2.y as x, ref$4.y as y
          FROM recursive$3 as ref$5, recursive$1 as ref$4, abs as abs$2, app as app$2
          WHERE ref$5.x = app$2.y AND ref$5.y = abs$2.x AND ref$4.x = app$2.z))), recursive$3 AS
        ((SELECT term$4.x as x, term$4.z as y
        FROM term as term$4
        WHERE term$4.y = 'Abs')
      UNION ALL
          ((SELECT term$5.x as x, ref$6.y as y
          FROM term as term$5, recursive$4 as ref$6
          WHERE term$5.y = 'Var' AND term$5.z = ref$6.x)
        UNION ALL
          (SELECT term$6.x as x, ref$7.y as y
          FROM term as term$6, recursive$3 as ref$7, recursive$3 as ref$8, abs as abs$3, app as app$3
          WHERE term$6.y = 'App' AND term$6.z = app$3.x AND ref$7.x = abs$3.z AND ref$8.x = app$3.y AND ref$8.y = abs$3.x))), recursive$4 AS
        ((SELECT *
        FROM baseCtrl as baseCtrl$1)
      UNION ALL
          ((SELECT abs$4.y as x, ref$9.y as y
          FROM recursive$3 as ref$10, recursive$3 as ref$9, abs as abs$4, app as app$4
          WHERE ref$10.x = app$4.y AND ref$10.y = abs$4.x AND ref$9.x = app$4.z)))
    SELECT DISTINCT COUNT(1)
    FROM recursive$1 as recref$1
    """

  override def postgres: String =
    """
    WITH RECURSIVE recursive$1 AS
        ((SELECT term$1.x as x, lits$1.y as y
        FROM term as term$1, lits as lits$1
        WHERE lits$1.x = term$1.z AND term$1.y = 'Lit')
      UNION ALL
          ((SELECT term$2.x as x, ref$1.y as y
          FROM term as term$2, recursive$2 as ref$1
          WHERE term$2.y = 'Var' AND term$2.z = ref$1.x)
        UNION ALL
          (SELECT term$3.x as x, ref$2.y as y
          FROM term as term$3, recursive$1 as ref$2, recursive$3 as ref$3, abs as abs$1, app as app$1
          WHERE term$3.y = 'App' AND term$3.z = app$1.x AND ref$2.x = abs$1.z AND ref$3.x = app$1.y AND ref$3.y = abs$1.x))), recursive$2 AS
        ((SELECT *
        FROM baseData as baseData$1)
      UNION ALL
          ((SELECT abs$2.y as x, ref$4.y as y
          FROM recursive$3 as ref$5, recursive$1 as ref$4, abs as abs$2, app as app$2
          WHERE ref$5.x = app$2.y AND ref$5.y = abs$2.x AND ref$4.x = app$2.z))), recursive$3 AS
        ((SELECT term$4.x as x, term$4.z as y
        FROM term as term$4
        WHERE term$4.y = 'Abs')
      UNION ALL
        (WITH recursive$3 AS
          (SELECT *
          FROM recursive$3)
          (SELECT term$5.x as x, ref$6.y as y
          FROM term as term$5, recursive$4 as ref$6
          WHERE term$5.y = 'Var' AND term$5.z = ref$6.x)
        UNION ALL
          (SELECT term$6.x as x, ref$7.y as y
          FROM term as term$6, recursive$3 as ref$7, recursive$3 as ref$8, abs as abs$3, app as app$3
          WHERE term$6.y = 'App' AND term$6.z = app$3.x AND ref$7.x = abs$3.z AND ref$8.x = app$3.y AND ref$8.y = abs$3.x))), recursive$4 AS
        ((SELECT *
        FROM baseCtrl as baseCtrl$1)
      UNION ALL
          ((SELECT abs$4.y as x, ref$9.y as y
          FROM recursive$3 as ref$10, recursive$3 as ref$9, abs as abs$4, app as app$4
          WHERE ref$10.x = app$4.y AND ref$10.y = abs$4.x AND ref$9.x = app$4.z)))
    SELECT DISTINCT COUNT(1)
    FROM recursive$1 as recref$1
    """

  override def sqlite: String =
    """
    WITH RECURSIVE recursive$1 AS
      (SELECT term$1.x as x, lits$1.y as y
      FROM term as term$1, lits as lits$1
      WHERE lits$1.x = term$1.z AND term$1.y = 'Lit'
      UNION ALL
      SELECT term$2.x as x, ref$1.y as y
      FROM term as term$2, recursive$2 as ref$1
      WHERE term$2.y = 'Var' AND term$2.z = ref$1.x
      UNION ALL
      SELECT term$3.x as x, ref$2.y as y
      FROM term as term$3, recursive$1 as ref$2, recursive$3 as ref$3, abs as abs$1, app as app$1
      WHERE term$3.y = 'App' AND term$3.z = app$1.x AND ref$2.x = abs$1.z AND ref$3.x = app$1.y AND ref$3.y = abs$1.x), recursive$2 AS
      (SELECT *
      FROM baseData as baseData$1
      UNION ALL
      SELECT abs$2.y as x, ref$4.y as y
      FROM recursive$3 as ref$5, recursive$1 as ref$4, abs as abs$2, app as app$2
      WHERE ref$5.x = app$2.y AND ref$5.y = abs$2.x AND ref$4.x = app$2.z), recursive$3 AS
      (SELECT term$4.x as x, term$4.z as y
      FROM term as term$4
      WHERE term$4.y = 'Abs'
      UNION ALL
      SELECT term$5.x as x, ref$6.y as y
      FROM term as term$5, recursive$4 as ref$6
      WHERE term$5.y = 'Var' AND term$5.z = ref$6.x
      UNION ALL
      SELECT term$6.x as x, ref$7.y as y
      FROM term as term$6, recursive$3 as ref$7, recursive$3 as ref$8, abs as abs$3, app as app$3
      WHERE term$6.y = 'App' AND term$6.z = app$3.x AND ref$7.x = abs$3.z AND ref$8.x = app$3.y AND ref$8.y = abs$3.x), recursive$4 AS
      (SELECT *
      FROM baseCtrl as baseCtrl$1
      UNION ALL
      SELECT abs$4.y as x, ref$9.y as y
      FROM recursive$3 as ref$10, recursive$3 as ref$9, abs as abs$4, app as app$4
      WHERE ref$10.x = app$4.y AND ref$10.y = abs$4.x AND ref$9.x = app$4.z)
    SELECT DISTINCT COUNT(1)
    FROM recursive$1 as recref$1
    """

// -------------------------------------------------------------------- SSSP

class SSSPBackendTest extends RQBBackendTest:
  def testDescription: String = "RQB sssp"

  def query() =
    WeightedGraphDBs.tables.base.unrestrictedFix(sp =>
      WeightedGraphDBs.tables.edge.flatMap(edge =>
        sp
          .filter(s => s.dst == edge.src)
          .map(s => (dst = edge.dst, cost = s.cost + edge.cost).toRow)
      ).distinct
    ).aggregate(s => (dst = s.dst, cost = min(s.cost)).toGroupingRow)
      .groupBySource(s => (dst = s._1.dst).toRow)

  def duckdb: String =
    """
    WITH RECURSIVE recursive$1 AS
        ((SELECT *
        FROM base as base$1)
      UNION
          ((SELECT edge$1.dst as dst, ref$1.cost + edge$1.cost as cost
          FROM edge as edge$1, recursive$1 as ref$1
          WHERE ref$1.dst = edge$1.src)))
    SELECT recref$1.dst as dst, MIN(recref$1.cost) as cost
    FROM recursive$1 as recref$1
    GROUP BY recref$1.dst
    """

  override def sqlite: String =
    """
    WITH RECURSIVE recursive$1 AS
      (SELECT *
      FROM base as base$1
      UNION
      SELECT edge$1.dst as dst, ref$1.cost + edge$1.cost as cost
      FROM edge as edge$1, recursive$1 as ref$1
      WHERE ref$1.dst = edge$1.src)
    SELECT recref$1.dst as dst, MIN(recref$1.cost) as cost
    FROM recursive$1 as recref$1
    GROUP BY recref$1.dst
    """

// --------------------------------------------------------- Same Generation

class SameGenerationBackendTest extends RQBBackendTest:
  def testDescription: String = "RQB samegeneration"

  def query() =
    val base = ancestryDBs.tables.parent
      .filter(p => p.parent == "Alice")
      .map(e => (name = e.child, gen = IntLit(1)).toRow)
    base.unrestrictedFix(gen =>
      ancestryDBs.tables.parent.flatMap(parent =>
        gen
          .filter(g => parent.parent == g.name)
          .map(g => (name = parent.child, gen = g.gen + 1).toRow)
      ).distinct
    ).filter(g => g.gen == 2).map(g => (name = g.name).toRow)

  def duckdb: String =
    """
    WITH RECURSIVE recursive$1 AS
        ((SELECT parents$1.child as name, 1 as gen
        FROM parents as parents$1
        WHERE parents$1.parent = 'Alice')
      UNION
          ((SELECT parents$2.child as name, ref$1.gen + 1 as gen
          FROM parents as parents$2, recursive$1 as ref$1
          WHERE parents$2.parent = ref$1.name)))
    SELECT recref$1.name as name
    FROM recursive$1 as recref$1
    WHERE recref$1.gen = 2
    """

  override def sqlite: String =
    """
    WITH RECURSIVE recursive$1 AS
      (SELECT parents$1.child as name, 1 as gen
      FROM parents as parents$1
      WHERE parents$1.parent = 'Alice'
      UNION
      SELECT parents$2.child as name, ref$1.gen + 1 as gen
      FROM parents as parents$2, recursive$1 as ref$1
      WHERE parents$2.parent = ref$1.name)
    SELECT recref$1.name as name
    FROM recursive$1 as recref$1
    WHERE recref$1.gen = 2
    """

// ----------------------------------------------------- Andersen's Points-To

class AndersensBackendTest extends RQBBackendTest:
  def testDescription: String = "RQB andersens"

  def query() =
    val base = AndersenPointsToDBs.tables.addressOf.map(a => (x = a.x, y = a.y).toRow)
    base.unrestrictedFix(pointsTo =>
      AndersenPointsToDBs.tables.assign.flatMap(a =>
        pointsTo.filter(p => a.y == p.x).map(p => (x = a.x, y = p.y).toRow))
      .union(AndersenPointsToDBs.tables.load.flatMap(l =>
        pointsTo.flatMap(pt1 =>
          pointsTo
            .filter(pt2 => l.y == pt1.x && pt1.y == pt2.x)
            .map(pt2 => (x = l.x, y = pt2.y).toRow))))
      .union(AndersenPointsToDBs.tables.store.flatMap(s =>
        pointsTo.flatMap(pt1 =>
          pointsTo
            .filter(pt2 => s.x == pt1.x && s.y == pt2.x)
            .map(pt2 => (x = pt1.y, y = pt2.y).toRow)))))

  // PostgreSQL: `query-postgres.sql` -- the recursive term names recursive$1 five times, so the
  // working table is shadowed by an inner CTE.
  def duckdb: String =
    """
    WITH RECURSIVE recursive$1 AS
        ((SELECT addressOf$1.x as x, addressOf$1.y as y
        FROM addressOf as addressOf$1)
      UNION
          ((SELECT assign$1.x as x, ref$1.y as y
          FROM assign as assign$1, recursive$1 as ref$1
          WHERE assign$1.y = ref$1.x)
        UNION
          (SELECT loadT$1.x as x, ref$2.y as y
          FROM loadT as loadT$1, recursive$1 as ref$3, recursive$1 as ref$2
          WHERE loadT$1.y = ref$3.x AND ref$3.y = ref$2.x)
        UNION
          (SELECT ref$4.y as x, ref$5.y as y
          FROM store as store$1, recursive$1 as ref$4, recursive$1 as ref$5
          WHERE store$1.x = ref$4.x AND store$1.y = ref$5.x)))
    SELECT *
    FROM recursive$1 as recref$1
    """

  override def postgres: String =
    """
    WITH RECURSIVE recursive$1 AS
        ((SELECT addressOf$1.x as x, addressOf$1.y as y
        FROM addressOf as addressOf$1)
      UNION
        (WITH recursive$1 AS
          (SELECT *
          FROM recursive$1)
          (SELECT assign$1.x as x, ref$1.y as y
          FROM assign as assign$1, recursive$1 as ref$1
          WHERE assign$1.y = ref$1.x)
        UNION
          (SELECT loadT$1.x as x, ref$2.y as y
          FROM loadT as loadT$1, recursive$1 as ref$3, recursive$1 as ref$2
          WHERE loadT$1.y = ref$3.x AND ref$3.y = ref$2.x)
        UNION
          (SELECT ref$4.y as x, ref$5.y as y
          FROM store as store$1, recursive$1 as ref$4, recursive$1 as ref$5
          WHERE store$1.x = ref$4.x AND store$1.y = ref$5.x)))
    SELECT *
    FROM recursive$1 as recref$1
    """

  override def sqlite: String =
    """
    WITH RECURSIVE recursive$1 AS
      (SELECT addressOf$1.x as x, addressOf$1.y as y
      FROM addressOf as addressOf$1
      UNION
      SELECT assign$1.x as x, ref$1.y as y
      FROM assign as assign$1, recursive$1 as ref$1
      WHERE assign$1.y = ref$1.x
      UNION
      SELECT loadT$1.x as x, ref$2.y as y
      FROM loadT as loadT$1, recursive$1 as ref$3, recursive$1 as ref$2
      WHERE loadT$1.y = ref$3.x AND ref$3.y = ref$2.x
      UNION
      SELECT ref$4.y as x, ref$5.y as y
      FROM store as store$1, recursive$1 as ref$4, recursive$1 as ref$5
      WHERE store$1.x = ref$4.x AND store$1.y = ref$5.x)
    SELECT *
    FROM recursive$1 as recref$1
    """

// -------------------------------------------------------------------- APSP

class APSPBackendTest extends RQBBackendTest:
  def testDescription: String = "RQB asps"

  def query() =
    val base = WeightedGraphDBs.tables.edge
      .aggregate(e => (src = e.src, dst = e.dst, cost = min(e.cost)).toGroupingRow)
      .groupBySource(e => (src = e._1.src, dst = e._1.dst).toRow)

    val asps = base.unrestrictedFix(path =>
      path.aggregate(p =>
        path
          .filter(e => p.dst == e.src)
          .aggregate(e => (src = p.src, dst = e.dst, cost = min(p.cost + e.cost)).toGroupingRow))
          .groupBySource(p => (g1 = p._1.src, g2 = p._2.dst).toRow).distinct
    )
    asps
      .aggregate(a => (src = a.src, dst = a.dst, cost = min(a.cost)).toGroupingRow)
      .groupBySource(p => (g1 = p._1.src, g2 = p._1.dst).toRow)

  // PostgreSQL: `query-postgres.sql` -- the recursive term self-joins the working table, so it is
  // shadowed by an inner CTE.
  def duckdb: String =
    """
    WITH RECURSIVE recursive$1 AS
        ((SELECT edge$1.src as src, edge$1.dst as dst, MIN(edge$1.cost) as cost
        FROM edge as edge$1
        GROUP BY edge$1.src, edge$1.dst)
      UNION
          ((SELECT ref$1.src as src, ref$2.dst as dst, MIN(ref$1.cost + ref$2.cost) as cost
          FROM recursive$1 as ref$1, recursive$1 as ref$2
          WHERE ref$1.dst = ref$2.src
          GROUP BY ref$1.src, ref$2.dst)))
    SELECT recref$1.src as src, recref$1.dst as dst, MIN(recref$1.cost) as cost
    FROM recursive$1 as recref$1
    GROUP BY recref$1.src, recref$1.dst
    """

  override def postgres: String =
    """
    WITH RECURSIVE recursive$1 AS
        ((SELECT edge$1.src as src, edge$1.dst as dst, MIN(edge$1.cost) as cost
        FROM edge as edge$1
        GROUP BY edge$1.src, edge$1.dst)
      UNION
        (WITH recursive$1 AS
          (SELECT *
          FROM recursive$1)
          (SELECT ref$1.src as src, ref$2.dst as dst, MIN(ref$1.cost + ref$2.cost) as cost
          FROM recursive$1 as ref$1, recursive$1 as ref$2
          WHERE ref$1.dst = ref$2.src
          GROUP BY ref$1.src, ref$2.dst)))
    SELECT recref$1.src as src, recref$1.dst as dst, MIN(recref$1.cost) as cost
    FROM recursive$1 as recref$1
    GROUP BY recref$1.src, recref$1.dst
    """

  override def sqlite: String =
    """
    WITH RECURSIVE recursive$1 AS
      (SELECT edge$1.src as src, edge$1.dst as dst, MIN(edge$1.cost) as cost
      FROM edge as edge$1
      GROUP BY edge$1.src, edge$1.dst
      UNION
      SELECT ref$1.src as src, ref$2.dst as dst, MIN(ref$1.cost + ref$2.cost) as cost
      FROM recursive$1 as ref$1, recursive$1 as ref$2
      WHERE ref$1.dst = ref$2.src
      GROUP BY ref$1.src, ref$2.dst)
    SELECT recref$1.src as src, recref$1.dst as dst, MIN(recref$1.cost) as cost
    FROM recursive$1 as recref$1
    GROUP BY recref$1.src, recref$1.dst
    """

// ---------------------------------------------------------------------- TC

class TCBackendTest extends RQBBackendTest:
  def testDescription: String = "RQB tc"

  def query() =
    val pathBase = GraphDBs.tables.edge
      .filter(p => p.x == 1)
      .map(e => (startNode = e.x, endNode = e.y, path = List(e.x, e.y).toExpr).toRow)

    pathBase.unrestrictedBagFix(path =>
      path.flatMap(p =>
        GraphDBs.tables.edge
          .filter(e => e.x == p.endNode && !p.path.contains(e.y))
          .map(e => (startNode = p.startNode, endNode = e.y, path = p.path.append(e.y)).toRow)))
      .sort(p => p.path, Ord.ASC).sort(p => p.path.length, Ord.ASC)

  // DuckDB uses native list values; PostgreSQL and SQLite take the portable `query.sql` encoding
  // (a comma-delimited string built with `||`); MariaDB takes `query-mariadb.sql` (CONCAT, since
  // `||` is logical OR there, and a CAST that pins the recursive column's width).
  def duckdb: String =
    """
    WITH RECURSIVE recursive$1 AS
        ((SELECT edge$1.x as startNode, edge$1.y as endNode, [edge$1.x, edge$1.y] as path
        FROM edge as edge$1
        WHERE edge$1.x = 1)
      UNION ALL
          ((SELECT ref$1.startNode as startNode, edge$2.y as endNode, list_append(ref$1.path, edge$2.y) as path
          FROM recursive$1 as ref$1, edge as edge$2
          WHERE edge$2.x = ref$1.endNode AND NOT list_contains(ref$1.path, edge$2.y))))
    SELECT *
    FROM recursive$1 as recref$1
    ORDER BY length(recref$1.path) ASC, path ASC
    """

  override def postgres: String =
    """
    WITH RECURSIVE recursive$1 AS
        ((SELECT edge$1.x as startNode, edge$1.y as endNode, ',' || edge$1.x || ',' || edge$1.y || ',' as path
        FROM edge as edge$1
        WHERE edge$1.x = 1)
      UNION ALL
          ((SELECT ref$1.startNode as startNode, edge$2.y as endNode, ref$1.path || edge$2.y || ',' as path
          FROM recursive$1 as ref$1, edge as edge$2
          WHERE edge$2.x = ref$1.endNode AND NOT (ref$1.path LIKE '%,' || edge$2.y || ',%'))))
    SELECT *
    FROM recursive$1 as recref$1
    ORDER BY (LENGTH(recref$1.path) - LENGTH(REPLACE(recref$1.path, ',', ''))) ASC, path ASC
    """

  override def sqlite: String =
    """
    WITH RECURSIVE recursive$1 AS
      (SELECT edge$1.x as startNode, edge$1.y as endNode, ',' || edge$1.x || ',' || edge$1.y || ',' as path
      FROM edge as edge$1
      WHERE edge$1.x = 1
      UNION ALL
      SELECT ref$1.startNode as startNode, edge$2.y as endNode, ref$1.path || edge$2.y || ',' as path
      FROM recursive$1 as ref$1, edge as edge$2
      WHERE edge$2.x = ref$1.endNode AND NOT (ref$1.path LIKE '%,' || edge$2.y || ',%'))
    SELECT *
    FROM recursive$1 as recref$1
    ORDER BY (LENGTH(recref$1.path) - LENGTH(REPLACE(recref$1.path, ',', ''))) ASC, path ASC
    """

  override def mariadb: String =
    """
    WITH RECURSIVE recursive$1 AS
        ((SELECT edge$1.x as startNode, edge$1.y as endNode, CAST(CONCAT(',', edge$1.x, ',', edge$1.y, ',') AS CHAR(4000)) as path
        FROM edge as edge$1
        WHERE edge$1.x = 1)
      UNION ALL
          ((SELECT ref$1.startNode as startNode, edge$2.y as endNode, CONCAT(ref$1.path, edge$2.y, ',') as path
          FROM recursive$1 as ref$1, edge as edge$2
          WHERE edge$2.x = ref$1.endNode AND NOT (ref$1.path LIKE CONCAT('%,', edge$2.y, ',%')))))
    SELECT *
    FROM recursive$1 as recref$1
    ORDER BY (LENGTH(recref$1.path) - LENGTH(REPLACE(recref$1.path, ',', ''))) ASC, path ASC
    """

// --------------------------------------------------------------------- BOM

class BOMBackendTest extends RQBBackendTest:
  def testDescription: String = "RQB bom"

  def query() =
    BOMDBs.tables.basic.unrestrictedBagFix(waitFor =>
      BOMDBs.tables.assbl.flatMap(assbl =>
        waitFor
          .filter(wf => assbl.spart == wf.part)
          .map(wf => (part = assbl.part, days = wf.days).toRow)))
      .aggregate(wf => (part = wf.part, max = max(wf.days)).toGroupingRow)
      .groupBySource(wf => (part = wf._1.part).toRow)

  def duckdb: String =
    """
    WITH RECURSIVE recursive$1 AS
        ((SELECT *
        FROM basic as basic$1)
      UNION ALL
          ((SELECT assbl$1.part as part, ref$1.days as days
          FROM assbl as assbl$1, recursive$1 as ref$1
          WHERE assbl$1.spart = ref$1.part)))
    SELECT recref$1.part as part, MAX(recref$1.days) as max
    FROM recursive$1 as recref$1
    GROUP BY recref$1.part
    """

  override def sqlite: String =
    """
    WITH RECURSIVE recursive$1 AS
      (SELECT *
      FROM basic as basic$1
      UNION ALL
      SELECT assbl$1.part as part, ref$1.days as days
      FROM assbl as assbl$1, recursive$1 as ref$1
      WHERE assbl$1.spart = ref$1.part)
    SELECT recref$1.part as part, MAX(recref$1.days) as max
    FROM recursive$1 as recref$1
    GROUP BY recref$1.part
    """

// ------------------------------------------------------------------ Orbits

class OrbitsBackendTest extends RQBBackendTest:
  def testDescription: String = "RQB orbits"

  def query() =
    val orbits = PlanetaryDBs.tables.base.unrestrictedBagFix(orbits =>
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
                .map(o2 => (x = o1.x, y = o2.y).toRow))
            .filter(io => o.x == io.x && o.y == io.y)
            .nonEmpty
        )

  // PostgreSQL: `query-postgres.sql` -- the recursive term self-joins the working table, so it is
  // shadowed by an inner CTE.
  def duckdb: String =
    """
    WITH RECURSIVE recursive$1 AS
        ((SELECT *
        FROM base as base$1)
      UNION ALL
          ((SELECT ref$1.x as x, ref$2.y as y
          FROM recursive$1 as ref$1, recursive$1 as ref$2
          WHERE ref$1.y = ref$2.x)))
    SELECT *
    FROM recursive$1 as recref$1
    WHERE EXISTS
      (SELECT *
      FROM
        (SELECT ref$3.x as x, ref$4.y as y
        FROM recursive$1 as ref$3, recursive$1 as ref$4
        WHERE ref$3.y = ref$4.x) as subquery$1
      WHERE recref$1.x = subquery$1.x AND recref$1.y = subquery$1.y)
    """

  override def postgres: String =
    """
    WITH RECURSIVE recursive$1 AS
        ((SELECT *
        FROM base as base$1)
      UNION ALL
        (WITH recursive$1 AS
          (SELECT *
          FROM recursive$1)
          (SELECT ref$1.x as x, ref$2.y as y
          FROM recursive$1 as ref$1, recursive$1 as ref$2
          WHERE ref$1.y = ref$2.x)))
    SELECT *
    FROM recursive$1 as recref$1
    WHERE EXISTS
      (SELECT *
      FROM
        (SELECT ref$3.x as x, ref$4.y as y
        FROM recursive$1 as ref$3, recursive$1 as ref$4
        WHERE ref$3.y = ref$4.x) as subquery$1
      WHERE recref$1.x = subquery$1.x AND recref$1.y = subquery$1.y)
    """

  override def sqlite: String =
    """
    WITH RECURSIVE recursive$1 AS
      (SELECT *
      FROM base as base$1
      UNION ALL
      SELECT ref$1.x as x, ref$2.y as y
      FROM recursive$1 as ref$1, recursive$1 as ref$2
      WHERE ref$1.y = ref$2.x)
    SELECT *
    FROM recursive$1 as recref$1
    WHERE EXISTS
      (SELECT *
      FROM
        (SELECT ref$3.x as x, ref$4.y as y
        FROM recursive$1 as ref$3, recursive$1 as ref$4
        WHERE ref$3.y = ref$4.x) as subquery$1
      WHERE recref$1.x = subquery$1.x AND recref$1.y = subquery$1.y)
    """

// --------------------------------------------------------------- Data Flow

class DataflowBackendTest extends RQBBackendTest:
  def testDescription: String = "RQB dataflow"

  def query() =
    FlowDBs.tables.jumpOp
      .unrestrictedBagFix(flow =>
        flow.flatMap(f1 =>
          flow.filter(f2 => f1.b == f2.a).map(f2 => (a = f1.a, b = f2.b).toRow)))
      .flatMap(f =>
        FlowDBs.tables.readOp.flatMap(r =>
          FlowDBs.tables.writeOp
            .filter(w => w.opN == f.a && w.varN == r.varN && f.b == r.opN)
            .map(w => (r = r.opN, w = w.opN).toRow)))

  // PostgreSQL: `query-postgres.sql` -- the recursive term self-joins the working table, so it is
  // shadowed by an inner CTE.
  def duckdb: String =
    """
    WITH RECURSIVE recursive$1 AS
        ((SELECT *
        FROM jumpOp as jumpOp$1)
      UNION ALL
          ((SELECT ref$1.a as a, ref$2.b as b
          FROM recursive$1 as ref$1, recursive$1 as ref$2
          WHERE ref$1.b = ref$2.a)))
    SELECT readOp$1.opN as r, writeOp$1.opN as w
    FROM recursive$1 as recref$1, readOp as readOp$1, writeOp as writeOp$1
    WHERE writeOp$1.opN = recref$1.a AND writeOp$1.varN = readOp$1.varN AND recref$1.b = readOp$1.opN
    """

  override def postgres: String =
    """
    WITH RECURSIVE recursive$1 AS
        ((SELECT *
        FROM jumpOp as jumpOp$1)
      UNION ALL
        (WITH recursive$1 AS
          (SELECT *
          FROM recursive$1)
          (SELECT ref$1.a as a, ref$2.b as b
          FROM recursive$1 as ref$1, recursive$1 as ref$2
          WHERE ref$1.b = ref$2.a)))
    SELECT readOp$1.opN as r, writeOp$1.opN as w
    FROM recursive$1 as recref$1, readOp as readOp$1, writeOp as writeOp$1
    WHERE writeOp$1.opN = recref$1.a AND writeOp$1.varN = readOp$1.varN AND recref$1.b = readOp$1.opN
    """

  override def sqlite: String =
    """
    WITH RECURSIVE recursive$1 AS
      (SELECT *
      FROM jumpOp as jumpOp$1
      UNION ALL
      SELECT ref$1.a as a, ref$2.b as b
      FROM recursive$1 as ref$1, recursive$1 as ref$2
      WHERE ref$1.b = ref$2.a)
    SELECT readOp$1.opN as r, writeOp$1.opN as w
    FROM recursive$1 as recref$1, readOp as readOp$1, writeOp as writeOp$1
    WHERE writeOp$1.opN = recref$1.a AND writeOp$1.varN = readOp$1.varN AND recref$1.b = readOp$1.opN
    """
