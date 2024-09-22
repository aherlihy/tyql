package test.query.groupby
import test.SQLStringQueryTest
import test.query.{commerceDBs, AllCommerceDBs, Purchase}

import tyql.*
import language.experimental.namedTuples
import NamedTuple.*
import scala.language.implicitConversions
import tyql.Expr.{sum, avg}
import tyql.AggregationExpr.toRow

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
//class JoinGroupByTest extends SQLStringQueryTest[AllCommerceDBs, (avg: Int)] {
//  def testDescription = "GroupBy: GroupByJoin"
//  def query() =
//    testDB.tables.buyers.flatMap(b =>
//      testDB.tables.shipInfos.map(si =>
//        (name = b.name, shippingId = si.id).toRow
//      )
//    ).groupBy(
//      p => (name = p.name).toRow,
//      p => (avg = avg(p.shippingId)).toRow
//    )
//
//  def expectedQueryPattern = "SELECT buyers$A.name as name, shippingInfo$B.shippingDate as shippingDate FROM buyers as buyers$A, shippingInfo as shippingInfo$B"
//}