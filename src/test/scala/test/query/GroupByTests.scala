package test.query.select2tests
import test.SQLStringTest
import test.query.{commerceDBs, AllCommerceDBs, Purchase}

import tyql.*
import tyql.Expr.toRow
import language.experimental.namedTuples
import NamedTuple.*
import scala.language.implicitConversions

class GroupByTest extends SQLStringTest[AllCommerceDBs, Purchase] {
  def testDescription = "GroupBy: simple"

  def query() =
    testDB.tables.purchases.map(_.total.avg).groupBy(
      p => p.count,
      p => p.id == 10
    )

  def sqlString =
    """ SELECT AVG(purchases.total) FROM purchases GROUP BY purchases.count
        """
}

class SortGroupByTest extends SQLStringTest[AllCommerceDBs, Purchase] {
  def testDescription = "GroupBy: sort then GroupBy"
  def query() =
    testDB.tables.purchases.sort(_.count, Ord.ASC).take(5).groupBy(
      p => p.total.avg,
      p => p.id == 10
    )
  def sqlString = """
      """
}
class JoinGroupByTest extends SQLStringTest[AllCommerceDBs, (id: Int, total: Double)] {
  def testDescription = "GroupBy: GroupByJoin"
  def query() =
    for
      p1 <- testDB.tables.purchases.groupBy(_.total.sum, f => f.id == 1)
      p2 <- testDB.tables.products
      if p1.id == p2.id
    yield (id = p1.id, total = p2.price.avg).toRow
  def sqlString = """
        SELECT
          product1.name AS res_0,
          subquery0.res_1 AS res_1
        FROM (SELECT
            purchase0.product_id AS res_0,
            SUM(purchase0.total) AS res_1
          FROM purchase purchase0
          GROUP BY purchase0.product_id) subquery0
        JOIN product product1 ON (subquery0.res_0 = product1.id)
      """
}