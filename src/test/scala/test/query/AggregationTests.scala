package test.query.aggregation
import test.{SQLStringQueryTest, SQLStringAggregationTest}
import test.query.{commerceDBs,  AllCommerceDBs}

import tyql.*
import language.experimental.namedTuples
import NamedTuple.*
import scala.language.implicitConversions
import Expr.{sum, avg, max}

// Expression-based aggregation:
class FlatMapAggregationExprTest extends SQLStringAggregationTest[AllCommerceDBs, Double] {
  def testDescription: String = "Aggregation: flatMap + expr.sum"

  def query() =
    testDB.tables.products
      .flatMap(p => sum(p.price))

  def sqlString: String = "SELECT SUM(purchase.price) FROM purchase"
}

class AggregateAggregationExprTest extends SQLStringAggregationTest[AllCommerceDBs, Double] {
  def testDescription: String = "Aggregation: aggregate + expr.sum"

  def query() =
    testDB.tables.products
      .aggregate(p => sum(p.price))

  def sqlString: String = "SELECT SUM(purchase.price) FROM purchase"
}


class FlatMapProjectAggregationExprTest extends SQLStringAggregationTest[AllCommerceDBs, (s: Double)] {
  def testDescription: String = "Aggregation: flatMap + expr.sum with named tuple"

  def query() =
    testDB.tables.products
      .flatMap(p =>
        (s = sum(p.price)).toRow
      )
  def sqlString: String = "SELECT SUM(purchase.price) as s FROM purchase"
}

class AggregateProjectAggregationExprTest extends SQLStringAggregationTest[AllCommerceDBs, (s: Double)] {
  def testDescription: String = "Aggregation: aggregate + expr.sum with named tuple"

  def query() =
    testDB.tables.products
      .aggregate(p =>
        (s = sum(p.price)).toRow
      )

  def sqlString: String = "SELECT SUM(purchase.price) as s FROM purchase"
}

class AggregateProjectAggregationExprConvertTest extends SQLStringAggregationTest[AllCommerceDBs, (s: Double)] {
  def testDescription: String = "Aggregation: aggregate + expr.sum with named tuple, auto convert toRow"

  def query() =
    testDB.tables.products
      .aggregate(p =>
        (s = sum(p.price))//.toRow
      )

  def sqlString: String = "SELECT SUM(purchase.price) as s FROM purchase"
}

class MapAggregationExprTest extends SQLStringQueryTest[AllCommerceDBs, Double] {
  def testDescription: String = "Aggregation: map + expr.sum, return query"

  def query() =
    testDB.tables.products
      .map(p => sum(p.price))

  def sqlString: String = "SELECT SUM(purchase.price) FROM purchase"
}

class MapProjectAggregationExprTest extends SQLStringQueryTest[AllCommerceDBs, (sum: Double)] {
  def testDescription: String = "Aggregation: map + expr.sum with named tuple"

  def query() =
    testDB.tables.products
      .map(p => (sum = sum(p.price)).toRow)

  def sqlString: String = "SELECT SUM(purchase.price) FROM purchase"
}

class FlatMapMultiAggregateTest extends SQLStringAggregationTest[AllCommerceDBs, (sum: Double, avg: Double)] {
  def testDescription: String = "Aggregation: filter then flatMap with named tuple)"

  def query() =
    testDB.tables.products
      .withFilter(p =>
        p.price != 0
      )
      .flatMap(p => (sum = sum(p.price), avg = avg(p.price)).toRow)

  def sqlString: String =
    """SELECT SUM(purchase.price), AVG(purchase.price)
          FROM purchase
    """
}

class AggregateMultiAggregateTest extends SQLStringAggregationTest[AllCommerceDBs, (sum: Double, avg: Double)] {
  def testDescription: String = "Aggregation: filter then aggregate with named tuple)"

  def query() =
    testDB.tables.products
      .withFilter(p =>
        p.price != 0
      )
      .aggregate(p => (sum = sum(p.price), avg = avg(p.price)).toRow)

  def sqlString: String =
    """SELECT SUM(purchase.price), AVG(purchase.price)
            FROM purchase
      """
}

class MapFilterAggregationExprTest extends SQLStringQueryTest[AllCommerceDBs, (sum: Double)] {
  def testDescription: String = "Aggregation: filter then map with named tuple"
  def query() =
    testDB.tables.products
      .withFilter(p =>
        p.price != 0
      )
      .map(p => (sum = sum(p.price)).toRow)

  def sqlString: String = """
        SELECT SUM(purchase.price)
        FROM purchase WHERE purchase.price != 0
      """
}

// Query helper-method based aggregation:
class AggregationQueryTest extends SQLStringAggregationTest[AllCommerceDBs, Double] {
  def testDescription: String = "Aggregation: sum on query"
  def query() =
    testDB.tables.products.sum(_.price)
  def sqlString: String = "SELECT SUM(product.price) FROM product"
}

class FilterAggregationQueryTest extends SQLStringAggregationTest[AllCommerceDBs, Double] {
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

class FilterAggregationProjectQueryTest extends SQLStringAggregationTest[AllCommerceDBs, (sum: Double)] {
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

class FilterMapAggregationQuerySelectTest extends SQLStringAggregationTest[AllCommerceDBs, Double] {
  def testDescription: String = "Aggregation: sum with map"
  def query() =
    testDB.tables.products
      .withFilter(p => p.price != 0)
      .map(p => (newPrice = p.price).toRow)
      .sum(_.newPrice)
  def sqlString: String = """
  SELECT SUM(p.newPrice) FROM (SELECT product.price as newPrice
          FROM product
          WHERE product.price > 0)
  """
}

class AggregationSubqueryTest extends SQLStringQueryTest[AllCommerceDBs, Boolean] {
  def testDescription: String = "Aggregation: regular query with aggregate subquery as expr (returns query)"
  def query() =
    testDB.tables.products
      .map(p => p.price > testDB.tables.products.avg(_.price))

  def sqlString: String = """
        SELECT product.price > (SELECT avg(product.price) FROM products)
        FROM product
      """
}

// TODO: this is a bit strange but most closely resembles SQL behavior
class AggregationSubqueryTest2 extends SQLStringQueryTest[AllCommerceDBs, Boolean] {
  def testDescription: String = "Aggregation: query with aggregation as expr (returns )"
  def query() =
    testDB.tables.products
      .map(p => p.price > avg(p.price))

  def sqlString: String = """
        SELECT product.price > avg(product.price)
        FROM product
      """
}

//class AggregationSubqueryTest3 extends SQLStringAggregationTest[AllCommerceDBs, Boolean] {
//  def testDescription: String = "Aggregation: aggregate with aggregation as expr (returns aggregate)"
//
//  def query() =
//    testDB.tables.products
//      .aggregate(p => p.price > avg(p.price))
//
//  def sqlString: String =
//    """
//          SELECT product.price > avg(product.price)
//          FROM product
//        """
//}

class AggregationSubqueryTest4 extends SQLStringQueryTest[AllCommerceDBs, Boolean] {
  def testDescription: String = "Aggregation: regular query with aggregate subquery as source (returns query)"
  def query() =
    testDB.tables.products.map(p => (avgPrice = avg(p.price))).map(r => r == 10)

  def sqlString: String = """
        SELECT avgPrice = 10
        FROM (SELECT AVG(price) as avgPrice FROM products)
      """
}

class AggregationSubqueryTest5 extends SQLStringQueryTest[AllCommerceDBs, Double] {
  def testDescription: String = "Aggregation: aggregation with aggregate subquery as source (returns aggregation)"
  def query() =
    testDB.tables.products.map(p => (avgPrice = avg(p.price))).map(r => max(r.avgPrice))

  def sqlString: String = """
        SELECT MAX(avgPrice)
        FROM (SELECT AVG(price) as avgPrice FROM products)
      """
}