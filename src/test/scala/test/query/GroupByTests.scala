package test.query.groupby
import test.SQLStringQueryTest
import test.query.recursive.{SSSPDB, SSSPEdge, SSSPDBs}
import test.query.{AllCommerceDBs, Purchase, commerceDBs}
import tyql.*

import language.experimental.namedTuples
import NamedTuple.*
import scala.language.implicitConversions
import tyql.Expr.{avg, min, sum}

class GroupByTest extends SQLStringQueryTest[AllCommerceDBs, (total: Double)] {
  def testDescription = "GroupBy: simple"

  // TODO: abstract so all DB has the same dbSetup
  def dbSetup: String =
    """
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
    testDB.tables.purchases.groupBy(
      p => (count = p.count).toRow,
      p => (total = avg(p.total)).toRow
    )

  def expectedQueryPattern: String =
    """SELECT AVG(purchase$0.total) as total FROM purchase as purchase$0 GROUP BY purchase$0.count"""
}

// NOTE: this should fail since can't groupBy without named field
//class GroupByUnamedTest extends SQLStringQueryTest[AllCommerceDBs, Double] {
//  def testDescription = "GroupBy: simple without named tuple"
//
//  def query() =
//      testDB.tables.purchases.groupBy(
//        p => avg(p.total),
//        p => p.count,
//        p => p == 1
//      )
//def expectedQueryPattern: String =
//    """SELECT AVG(purchases.total) FROM purchases GROUP BY purchases.count"""
//}

// TODO: alternative design, without .having or multiple arguments to groupBy
//  NOTE: this should fail because flatMap out the .count field, then try to group by it
//class GroupBy3FailTest extends SQLStringQueryTest[AllCommerceDBs, Purchase] {
//  def testDescription = "GroupBy: simple with having"
//
//  def query() =
//    testDB.tables.purchases.flatMap(p => (avg = avg(p.total)).toRow).groupBy(
//      p => p.count,
//    ).filter(p => p.avg == 10)
//
//def expectedQueryPattern: String =
//    """SELECT AVG(purchases.total) AS avg FROM purchases GROUP BY purchases.count HAVING avg = 10"""
//}

class GroupBy2Test extends SQLStringQueryTest[AllCommerceDBs, (avg: Double)] {
  def testDescription = "GroupBy: simple with having"

  def query() =
    import AggregationExpr.toRow
    testDB.tables.purchases
      .groupBy(
        p => (count = p.count).toRow,
        p => (avg = avg(p.total)).toRow)
      .having(
        p => avg(p.total) == 1
      )

  def expectedQueryPattern: String =
    """SELECT AVG(purchase$0.total) as avg FROM purchase as purchase$0 GROUP BY purchase$0.count HAVING AVG(purchase$0.total) = 1"""
}

class GroupBy3Test extends SQLStringQueryTest[AllCommerceDBs, (avg: Double)] {
  def testDescription = "GroupBy: simple with filter, triggers subquery"

  def query() =
    testDB.tables.purchases
      .groupBy(
        p => (count = p.count).toRow,
        p => (avg = avg(p.total)).toRow)
      .filter(
        p => p.avg == 1
      )

  def expectedQueryPattern: String =
    """SELECT * FROM (SELECT AVG(purchase$0.total) as avg FROM purchase as purchase$0 GROUP BY purchase$0.count) as subquery$1 WHERE subquery$1.avg = 1"""
}

class GroupBy4Test extends SQLStringQueryTest[AllCommerceDBs, (avg: Double)] {
  def testDescription = "GroupBy: simple with PRE-filter"

  def query() =
    testDB.tables.purchases
      .filter(p => p.id > 10)
      .groupBy(
        p => (count = p.count).toRow,
        p => (avg = avg(p.total)).toRow)
      .having(
        p => p.count == 1
      )

  def expectedQueryPattern: String =
    """SELECT AVG(purchase$0.total) as avg FROM purchase as purchase$0 WHERE purchase$0.id > 10 GROUP BY purchase$0.count HAVING purchase$0.count = 1"""
}

class GroupBy5Test extends SQLStringQueryTest[AllCommerceDBs, (avg: Double)] {
  def testDescription = "GroupBy: simple with filter and distinct"

  def query() =
    testDB.tables.purchases
      .filter(p => p.id > 10)
      .groupBy(
        p => (count = p.count).toRow,
        p => (avg = avg(p.total)).toRow)
      .having(
        p => p.count == 1
      ).distinct

  def expectedQueryPattern: String =
    """SELECT DISTINCT AVG(purchase$0.total) as avg FROM purchase as purchase$0 WHERE purchase$0.id > 10 GROUP BY purchase$0.count HAVING purchase$0.count = 1"""
}

class GroupBy6Test extends SQLStringQueryTest[AllCommerceDBs, (avg: Double)] {
  def testDescription = "GroupBy: simple with filter and distinct then sort"

  def query() =
    testDB.tables.purchases
      .filter(p => p.id > 10)
      .groupBy(
        p => (count = p.count).toRow,
        p => (avg = avg(p.total)).toRow)
      .having(
        p => p.count == 1
      ).distinct.sort(_.avg, Ord.ASC)

  def expectedQueryPattern: String =
    """SELECT DISTINCT AVG(purchase$0.total) as avg FROM purchase as purchase$0 WHERE purchase$0.id > 10 GROUP BY purchase$0.count HAVING purchase$0.count = 1 ORDER BY avg ASC"""
}

class GroupBy7Test extends SQLStringQueryTest[AllCommerceDBs, (avg: Double)] {
  def testDescription = "GroupBy: force subquery in groupBy using sort"

  def query() =
    testDB.tables.purchases.sort(_.id, Ord.ASC)
      .groupBy(
        p => (count = p.count).toRow,
        p => (avg = avg(p.total)).toRow)

  def expectedQueryPattern: String =
    """SELECT AVG(subquery$1.total) as avg FROM (SELECT * FROM purchase as purchase$0 ORDER BY id ASC) as subquery$1 GROUP BY subquery$1.count"""
}

class GroupBy8Test extends SQLStringQueryTest[AllCommerceDBs, (avg: Double, avgNum: Int)] {
  def testDescription = "GroupBy: simple with having, mixed scalar result"

  def query() =
    testDB.tables.purchases
      .groupBy(
        p => (count = p.count).toRow,
        p => {
          val agg = (avg = avg(p.total), avgNum = p.count)
          agg.toRow
        }
      )
      .having(
        p => avg(p.total) == 1
      )

  def expectedQueryPattern: String =
    """SELECT AVG(purchase$0.total) as avg, purchase$0.count as avgNum FROM purchase as purchase$0 GROUP BY purchase$0.count HAVING AVG(purchase$0.total) = 1"""
}

class GroupBy9Test extends SQLStringQueryTest[AllCommerceDBs, (avg: Double, avgNum: Int)] {
  def testDescription = "GroupBy: simple with having, mixed scalar result"

  def query() =
    testDB.tables.purchases
      .groupByAggregate(
        p => (count = avg(p.count)).toRow,
        p => {
          val agg = (avg = avg(p.total), avgNum = p.count)
          agg.toRow
        }
      )
      .having(
        p => avg(p.total) == 1
      )

  def expectedQueryPattern: String =
    """SELECT AVG(purchase$0.total) as avg, purchase$0.count as avgNum FROM purchase as purchase$0 GROUP BY AVG(purchase$0.count) HAVING AVG(purchase$0.total) = 1"""
}

// TODO: not yet implemented
//class GroupBy*Test extends SQLStringQueryTest[AllCommerceDBs, (avg: Double)] {
//  def testDescription = "GroupBy: groupBy not-named tuple"
//
//  def query() =
//    testDB.tables.purchases.sort(_.id, Ord.ASC)
//      .groupBy(
//        p => p.count,
//        p => (avg = avg(p.total)).toRow)
//
//  def expectedQueryPattern: String =
//    """SELECT AVG(subquery$1.total) as avg FROM purchase as purchase$0 GROUP BY purchase$0.count"""
//}

// TODO: Not yet implemneted, either force subquery or merge project statements
class JoinGroupByTest extends SQLStringQueryTest[AllCommerceDBs, (newId: Int, newName: String)] {
  def testDescription = "GroupBy: GroupByJoin"
  def query() =
    testDB.tables.buyers.aggregate(b =>
      testDB.tables.shipInfos.aggregate(si =>
        (newId = sum(si.id), newName = b.name).toGroupingRow
      )
    ).groupBySource(
      (buy, ship) =>
        (name = ship.shippingDate).toRow,
    )

  def expectedQueryPattern = """
    SELECT SUM(shippingInfo$B.id) as newId, buyers$A.name as newName
    FROM buyers as buyers$A, shippingInfo as shippingInfo$B
    GROUP BY shippingInfo$B.shippingDate
  """
}

class JoinGroupByTest1 extends SQLStringQueryTest[AllCommerceDBs, (newId: Int)] {
  def testDescription = "GroupBy: GroupByJoin"
  def query() =
    testDB.tables.shipInfos.aggregate(si =>
      (newId = sum(si.id)).toGroupingRow
    ).groupBySource(
      p =>
        (name = p._1.id).toRow,
    )

  def expectedQueryPattern = """
    SELECT SUM(shippingInfo$A.id) as newId
    FROM shippingInfo as shippingInfo$A
    GROUP BY shippingInfo$A.id
  """
}

class JoinGroupByTest2 extends SQLStringQueryTest[AllCommerceDBs, (newId: Int)] {
  def testDescription = "GroupBy: GroupByJoin"
  def query() =
    testDB.tables.shipInfos.aggregate(si =>
      (newId = sum(si.id)).toGroupingRow
    ).groupBySource(
      p =>
        (name = sum(p._1.id)).toRow,
    )

  def expectedQueryPattern = """
    SELECT SUM(shippingInfo$A.id) as newId
    FROM shippingInfo as shippingInfo$A
    GROUP BY SUM(shippingInfo$A.id)
  """
}

class JoinGroupByTest3 extends SQLStringQueryTest[AllCommerceDBs, (newId1: Int, newId2: Int, min: Int)] {
  def testDescription = "GroupBy: GroupByJoin"
  def query() =
    testDB.tables.buyers.aggregate(b1 =>
      testDB.tables.buyers.aggregate(b2 =>
        (newId1 = b1.id, newId2 = b2.id, min = min(b1.id + b2.id)).toGroupingRow
      )
    ).groupBySource(
      p =>
        (g1 = p._1.id, g2 = p._2.id).toRow,
    )

  def expectedQueryPattern = """
    SELECT buyers$A.id as newId1, buyers$B.id as newId2, MIN(buyers$A.id + buyers$B.id) as min
    FROM buyers as buyers$A, buyers as buyers$B
    GROUP BY buyers$A.id, buyers$B.id
  """
}

class JoinGroupByTest4 extends SQLStringQueryTest[SSSPDB, SSSPEdge] {
  def testDescription = "GroupBy: GroupByJoin"
  def query() =
    val path = testDB.tables.edge
    path.aggregate(p =>
      path
        .filter(e => p.dst == e.src)
        .aggregate(e => (src = p.src, dst = e.dst, cost = min(p.cost + e.cost)).toGroupingRow)
    ).groupBySource(
      p =>
        (g1 = p._1.src, g2 = p._2.dst).toRow
    )

  def expectedQueryPattern = """
     SELECT edge$16.src as src, edge$17.dst as dst, MIN(edge$16.cost + edge$17.cost) as cost
     FROM edge as edge$16, edge as edge$17
     WHERE edge$16.dst = edge$17.src
     GROUP BY edge$16.src, edge$17.dst
  """
}