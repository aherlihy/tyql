package test.query.select2tests
import test.SQLStringTest
import test.query.{commerceDBs, AllCommerceDBs, Purchase}

import tyql.*
import tyql.Expr.toRow
import language.experimental.namedTuples
import NamedTuple.*
import scala.language.implicitConversions

//class GroupByTest extends SQLStringTest[AllCommerceDBs, Int] {
//  def testDescription = "GroupBy: simple"
//
//  def query() =
//    testDB.tables.purchases.flatMap(p => (a = p.total.avg).toRow).groupBy(
//      p => p.a
//    )
//
//  def sqlString =
//    """SELECT AVG(purchases.total) FROM purchases GROUP BY purchases.count"""
//}

//class GroupByUnamedTest extends SQLStringTest[AllCommerceDBs, Int] {
//  def testDescription = "GroupBy: simple without named tuple"
//
//  def query() =
//    testDB.tables.purchases.flatMap(p => p.total.avg).groupBy(_)
//
//  def sqlString =
//    """SELECT AVG(purchases.total) FROM purchases GROUP BY purchases.count"""
//}

// TODO: use seq to avoid chaining, update sort with the same
//class GroupBy2Test extends SQLStringTest[AllCommerceDBs, Purchase] {
//  def testDescription = "GroupBy: simple multiple keys"
//
//  def query() =
//    testDB.tables.purchases.flatMap(p => (a = _.total.avg)).groupBy(
//      p => (p.count, p.total)
//    )
//
//  def sqlString =
//    """SELECT AVG(purchases.total) FROM purchases GROUP BY purchases.count, purchases.total"""
//}

//class GroupBy3Test extends SQLStringTest[AllCommerceDBs, Purchase] {
//  def testDescription = "GroupBy: simple with having"
//
//  def query() =
//    testDB.tables.purchases.flatMap(p => (avg = p.total.avg).toRow).groupBy(
//      p => p.count,
//    ).filter(p => p.avg == 10)
//
//  def sqlString =
//    """SELECT AVG(purchases.total) AS avg FROM purchases GROUP BY purchases.count HAVING avg = 10"""
//}
//
//class GroupBy4Test extends SQLStringTest[AllCommerceDBs, Purchase] {
//  def testDescription = "GroupBy: simple with filter"
//
//  def query() =
//    testDB.tables.purchases.filter(p => p.id == 10).flatMap(p => (a = p.total.avg)).groupBy(
//      p => p.a,
//    )
//
//  def sqlString =
//    """SELECT AVG(purchases.total) AS avg FROM purchases WHERE purchases.id = 10 GROUP BY AVG(purchases.count)"""
//}

// This should fail to compile because calling groupBy on non-aggregated result
//class SortGroupByTest extends SQLStringTest[AllCommerceDBs, Purchase] {
//  def testDescription = "GroupBy: sort then GroupBy"
//  def query() =
//    testDB.tables.purchases.sort(_.count, Ord.ASC).take(5).groupBy(
//      p => p.total.avg
//    )
//  def sqlString = """
//      """
//}
//class JoinGroupByTest extends SQLStringTest[AllCommerceDBs, (id: Int, total: Double)] {
//  def testDescription = "GroupBy: GroupByJoin"
//  def query() =
//    for
//      p1 <- testDB.tables.purchases.flatMap(p => (s = p.total.sum)).groupBy(_.total.sum, f => f.id == 1)
//      p2 <- testDB.tables.products
//      if p1.id == p2.id
//    yield (id = p1.id, total = p2.price.avg).toRow
//  def sqlString = """
//        SELECT
//          product1.name AS res_0,
//          subquery0.res_1 AS res_1
//        FROM (SELECT
//            purchase0.product_id AS res_0,
//            SUM(purchase0.total) AS res_1
//          FROM purchase purchase0
//          GROUP BY purchase0.product_id) subquery0
//        JOIN product product1 ON (subquery0.res_0 = product1.id)
//      """
//}