package test.query.aggregation
import test.SQLStringTest
import test.query.{commerceDBs,  AllCommerceDBs}

import tyql.*
import tyql.Expr.toRow
import language.experimental.namedTuples
import NamedTuple.*
import scala.language.implicitConversions

// Expression-based aggregation:

class FlatMapAggregationExprTest extends SQLStringTest[AllCommerceDBs, Double] {
  def testDescription: String = "Aggregation: flatMap + expr.sum"

  def query() =
    testDB.tables.products
      .flatMap(p => p.price.sum)

  def sqlString: String = "SELECT SUM(purchase.price) FROM purchase"
}

// TODO: these should compile?
//class FlatMapProjectAggregationExprTest extends SQLStringTest[AllCommerceDBs, Any] {
//  def testDescription: String = "Aggregation: flatMap + expr.sum with named tuple"
//
//  def query() =
//    testDB.tables.products
//      .flatMap(p =>
//        (sum = p.price.sum).toRow
//      )
//  def sqlString: String = "SELECT SUM(purchase.price) FROM purchase"
//}

class MapAggregationExprTest extends SQLStringTest[AllCommerceDBs, Double] {
  def testDescription: String = "Aggregation: map + expr.sum"

  def query() =
    testDB.tables.products
      .map(p => p.price.sum)

  def sqlString: String = "SELECT SUM(purchase.price) FROM purchase"
}

class MapProjectAggregationExprTest extends SQLStringTest[AllCommerceDBs, (sum: Double)] {
  def testDescription: String = "Aggregation: map + expr.sum with named tuple"

  def query() =
    testDB.tables.products
      .map(p => (sum = p.price.sum).toRow)

  def sqlString: String = "SELECT SUM(purchase.price) FROM purchase"
}

// TODO: same issue as above with flatMap + project
//class AggregationMultiAggregateTest extends SQLStringTest[AllCommerceDBs, (sum: Double, avg: Double)] {
//  def testDescription: String = "Aggregation: filter then flatMap with named tuple)"
//
//  def query() =
//    testDB.tables.products
//      .withFilter(p =>
//        p.price != 0
//      )
//      .flatMap(p => (sum = p.price.sum, avg = p.price.avg).toRow)
//
//  def sqlString: String =
//    """
//          SELECT SUM(purchase.price), AVG(purchase.price)
//          FROM purchase
//        """
//}

class MapFilterAggregationExprTest extends SQLStringTest[AllCommerceDBs, (sum: Double)] {
  def testDescription: String = "Aggregation: filter then map with named tuple"
  def query() =
    testDB.tables.products
      .withFilter(p =>
        p.price != 0
      )
      .map(p => (sum = p.price.sum).toRow)

  def sqlString: String = """
        SELECT SUM(purchase.price)
        FROM purchase WHERE purchase.price != 0
      """
}

// Query-based aggregation:
class AggregationQueryTest extends SQLStringTest[AllCommerceDBs, Double] {
  def testDescription: String = "Aggregation: sum on query"
  def query() =
    testDB.tables.products.sum(_.price)
  def sqlString: String = "SELECT SUM(product.price) FROM product"
}

class FilterAggregationQueryTest extends SQLStringTest[AllCommerceDBs, Double] {
  def testDescription: String = "Aggregation: filter then  sum on query"
  def query() =
    testDB.tables.products
      .withFilter(p =>
        p.price != 0
      )
      .sum(p => p.price)

  def sqlString: String = """
        SELECT SUM(purchase.price)
        FROM purchase WHERE purchase.price
      """
}

class FilterAggregationProjectQueryTest extends SQLStringTest[AllCommerceDBs, (sum: Double)] {
  def testDescription: String = "Aggregation: sum on query with tuple"
  def query() =
    testDB.tables.products
      .withFilter(p =>
        p.price != 0
      )
      .sum(p => (sum = p.price).toRow) // TODO: should sum only allow attributes instead of any expr?

  def sqlString: String = """
        SELECT SUM(purchase.price)
        FROM purchase WHERE purchase.price != 0
      """
}

class FilterMapAggregationQuerySelectTest extends SQLStringTest[AllCommerceDBs, Double] {
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