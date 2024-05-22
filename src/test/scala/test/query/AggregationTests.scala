package test.query.aggregation
import test.SQLStringTest
import test.query.{commerceDBs,  AllCommerceDBs}

import tyql.*
import tyql.Expr.toRow
import language.experimental.namedTuples
import NamedTuple.*
import scala.language.implicitConversions

class SimpleAggregationExprTest extends SQLStringTest[AllCommerceDBs, Double] {
  def testDescription: String = "Aggregation: sum on expr"

  def query() =
    testDB.tables.products
      .flatMap(p => p.price.sum)

  def sqlString: String = "SELECT SUM(purchase.price) FROM purchase"
}
// TODO: not sure if this should compile or not

//class SimpleAggregation2ExprTest extends SQLStringTest[AllCommerceDBs, Double] {
//  def testDescription: String = "Aggregation: sum on expr witih named tuple"
//
//  def query() =
//    testDB.tables.products
//      .flatMap(p => (sum = p.price.sum))
//  def sqlString: String = "SELECT SUM(purchase.price) FROM purchase"
//}
//
//class AggregationExprTest extends SQLStringTest[AllCommerceDBs, (sum: Double)] {
//  def testDescription: String = "Aggregation: sum on expr with condition"
//  def query() =
//    testDB.tables.products
//      .withFilter(p =>
//        p.price != 0
//      )
//      .map(p => (sum = p.price.sum).toRow)
//
//  def sqlString: String = """
//        SELECT SUM(purchase.price)
//        FROM purchase
//      """
//}

class AggregationQueryTest extends SQLStringTest[AllCommerceDBs, Double] {
  def testDescription: String = "Aggregation: sum on query"
  def query() =
    testDB.tables.products
      .withFilter(p =>
        p.price != 0
      )
      .sum(p => p.price)

  def sqlString: String = """
        SELECT SUM(purchase.price)
        FROM purchase
      """
}

class AggregationQueryProjectTest extends SQLStringTest[AllCommerceDBs, (sum: Double)] {
  def testDescription: String = "Aggregation: sum on query"
  def query() =
    testDB.tables.products
      .withFilter(p =>
        p.price != 0
      )
      .sum(p => (sum = p.price).toRow)

  def sqlString: String = """
        SELECT SUM(purchase.price)
        FROM purchase
      """
}


class AggregationSumSelectTest extends SQLStringTest[AllCommerceDBs, Double] {
  def testDescription: String = "Aggregation: sum"
  def query() =
    testDB.tables.products.sum(_.price)
  def sqlString: String = "SELECT SUM(product.price) FROM product"
}

class AggregationSum2SelectTest extends SQLStringTest[AllCommerceDBs, Double] {
  def testDescription: String = "Aggregation: sum with filter"
  def query() =
    testDB.tables.products.withFilter(p => p.price != 0).sum(_.price)
  def sqlString: String = "SELECT SUM(product.price) FROM product WHERE product.price > 0"
}

class AggregationSum3SelectTest extends SQLStringTest[AllCommerceDBs, Double] {
  def testDescription: String = "Aggregation: sum with map"
  def query() =
    testDB.tables.products.withFilter(p => p.price != 0).sum(_.price)
  def sqlString: String = "SELECT SUM(product.price) FROM product WHERE product.price > 0"
}

class AggregationSum4SelectTest extends SQLStringTest[AllCommerceDBs, Double] {
  def testDescription: String = "Aggregation: sum with map"
  def query() =
    testDB.tables.products
      .withFilter(p => p.price != 0)
      .map(p => (newPrice = p.price))
      .sum(_.newPrice)
  def sqlString: String = """
  SELECT SUM(p.newPrice) FROM (SELECT product.price as newPrice
          FROM product
          WHERE product.price > 0)
  """
}

class AggregationMultiAggregateTest extends SQLStringTest[AllCommerceDBs, (sum: Double, avg: Double)] {
  def testDescription: String = "Aggregation: aggregate"
  def query() =
    testDB.tables.products
      .withFilter(p =>
        p.price != 0
      )
      .map(p => (sum = p.price.sum, avg = p.price.avg).toRow)

  def sqlString: String = """
        SELECT SUM(purchase.price), AVG(purchase.price)
        FROM purchase
      """
}