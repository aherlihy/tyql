package test.query.aggregation
import test.{SQLStringAggregationTest, SQLStringQueryTest}
import test.query.{commerceDBs,  AllCommerceDBs}

import tyql.*
import language.experimental.namedTuples
import NamedTuple.*
import scala.language.implicitConversions
import Expr.{sum, avg, max}

// Expression-based aggregation:

class AggregateAggregationExprTest extends SQLStringAggregationTest[AllCommerceDBs, Double] {
  def testDescription: String = "Aggregation: aggregate + expr.sum"

  def query() =
    testDB.tables.products
      .aggregate(p => sum(p.price))

  def expectedQueryPattern: String = "SELECT SUM(product$A.price) FROM product as product$A"
}

class AggregateProjectAggregationExprTest extends SQLStringAggregationTest[AllCommerceDBs, (s: Double)] {
  def testDescription: String = "Aggregation: aggregate + expr.sum with named tuple"

  def query() =
    testDB.tables.products
      .aggregate(p =>
        (s = sum(p.price)).toRow
      )

  def expectedQueryPattern: String = "SELECT SUM(product$A.price) as s FROM product as product$A"
}

// TODO: toRow
//class AggregateProjectAggregationExprConvertTest extends SQLStringAggregationTest[AllCommerceDBs, (s: Double)] {
//  def testDescription: String = "Aggregation: aggregate + expr.sum with named tuple, auto convert toRow"
//
//  def query() =
//    testDB.tables.products
//      .aggregate(p =>
//        (s = sum(p.price))//.toRow
//      )
//
//  def expectedQueryPattern: String = "SELECT SUM(product$A.price) as s FROM product as product$A"
//}

// TODO: map with agg
//class MapAggregationExprTest extends SQLStringQueryTest[AllCommerceDBs, Double] {
//  def testDescription: String = "Aggregation: map + expr.sum, return query"
//
//  def query() =
//    testDB.tables.products
//      .map(p => sum(p.price))
//
//  def expectedQueryPattern: String = "SELECT SUM(product$A.price) FROM product as product$A"
//}
//
//class MapProjectAggregationExprTest extends SQLStringQueryTest[AllCommerceDBs, (sum: Double)] {
//  def testDescription: String = "Aggregation: map + expr.sum with named tuple"
//
//  def query() =
//    testDB.tables.products
//      .map(p => (sum = sum(p.price)).toRow)
//
//  def expectedQueryPattern: String = "SELECT SUM(product$A.price) as sum FROM product as product$A"
//}
//

class AggregateMultiAggregateTest extends SQLStringAggregationTest[AllCommerceDBs, (sum: Double, avg: Double)] {
  def testDescription: String = "Aggregation: filter then aggregate with named tuple, no subquery"

  def query() =
    testDB.tables.products
      .withFilter(p =>
        p.price != 0
      )
      .aggregate(p => (sum = sum(p.price), avg = avg(p.price)).toRow)

  def expectedQueryPattern: String =
    """SELECT SUM(product$A.price) as sum, AVG(product$A.price) as avg FROM product as product$A WHERE product$A.price <> 0
      """
}

// TODO: map w agg
//class MapFilterAggregationExprTest extends SQLStringQueryTest[AllCommerceDBs, (sum: Double)] {
//  def testDescription: String = "Aggregation: filter then map with named tuple"
//  def query() =
//    testDB.tables.products
//      .withFilter(p =>
//        p.price != 0
//      )
//      .map(p => (sum = sum(p.price)).toRow)
//
//  def expectedQueryPattern: String =
//    """SELECT SUM(product$A.price) as sum FROM product as product$A WHERE product$A.price <> 0
//      """
//}
//
//// Query helper-method based aggregation:
class AggregationQueryTest extends SQLStringAggregationTest[AllCommerceDBs, Double] {
  def testDescription: String = "Aggregation: sum on query"
  def query() =
    testDB.tables.products.sum(_.price)
  def expectedQueryPattern: String = "SELECT SUM(product$A.price) FROM product as product$A"
}

class FilterAggregationQueryTest extends SQLStringAggregationTest[AllCommerceDBs, Double] {
  def testDescription: String = "Aggregation: filter then sum on query"
  def query() =
    testDB.tables.products
      .withFilter(p =>
        p.price != 0
      )
      .sum(p => p.price)

  def expectedQueryPattern: String = """
  SELECT SUM(product$A.price) FROM product as product$A WHERE product$A.price <> 0
      """
}

// TODO
class FilterAggregationProjectQueryTest extends SQLStringAggregationTest[AllCommerceDBs, (sum: Double)] {
  def testDescription: String = "Aggregation: sum on query with tuple"
  def query() =
    testDB.tables.products
      .withFilter(p =>
        p.price != 0
      )
      .sum(p =>

        (sum = p.price).toRow
      )

  def expectedQueryPattern: String = """
  SELECT SUM(product$A.price as sum) FROM product as product$A WHERE product$A.price <> 0
      """
}

class FilterMapAggregationQuerySelectTest extends SQLStringAggregationTest[AllCommerceDBs, Double] {
  def testDescription: String = "Aggregation: sum with map"
  def query() =
    testDB.tables.products
      .withFilter(p => p.price != 0)
      .map(p => (newPrice = p.price).toRow)
      .sum(_.newPrice)
  def expectedQueryPattern: String = """
SELECT SUM(subquery$A.newPrice) FROM (SELECT product$B.price as newPrice FROM product as product$B WHERE product$B.price <> 0) as subquery$A
  """
}

class AggregationSubqueryTest extends SQLStringQueryTest[AllCommerceDBs, Boolean] {
  def testDescription: String = "Aggregation: regular query with aggregate subquery as expr (returns query)"
  def query() =
    val subquery = testDB.tables.products.sum(_.price)
    testDB.tables.products
      .map(p =>
        p.price > subquery
      )

  def expectedQueryPattern: String = """
    SELECT product$P.price > (SELECT SUM(product$B.price) FROM product as product$B) as subquery$D FROM product as product$P
      """
}

// TODO: map with agg
//class AggregationSubqueryTest4 extends SQLStringQueryTest[AllCommerceDBs, Boolean] {
//  def testDescription: String = "Aggregation: regular query with aggregate subquery as source (returns query)"
//  def query() =
//    testDB.tables.products.map(p =>
//      (avgPrice = avg(p.price))
//    ).map(r => r.avgPrice == 10)
//
//  def expectedQueryPattern: String = """
//        SELECT subquery$A.avgPrice = 10 FROM (SELECT AVG(product$B.price) as avgPrice FROM product as product$B) as subquery$A
//      """
//}
//
//class AggregationSubqueryTest5 extends SQLStringQueryTest[AllCommerceDBs, Double] {
//  def testDescription: String = "Aggregation: aggregation with aggregate subquery as source (returns aggregation)"
//  def query() =
//    testDB.tables.products.map(p =>
//      (avgPrice = avg(p.price))
//    ).map(r => max(r.avgPrice))
//
//  def expectedQueryPattern: String = """
//        SELECT MAX(subquery$A.avgPrice) FROM (SELECT AVG(product$B.price) as avgPrice FROM product as product$B) as subquery$A
//      """
//}

