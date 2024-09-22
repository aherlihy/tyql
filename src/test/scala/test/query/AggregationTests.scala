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

class AggregateAggregationExpr2Test extends SQLStringAggregationTest[AllCommerceDBs, (s: Double, t: Double)] {
  def testDescription: String = "Aggregation: aggregate + expr.sum + scalar, only for grouping"

  def query() =
    testDB.tables.products
      .aggregate(p => (s = sum(p.price), t = p.price).toGroupingRow)

  def expectedQueryPattern: String = "SELECT SUM(product$A.price) as s, product$A.price as t FROM product as product$A"
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

class AggregateMultiSubexpressionAggregateTest extends SQLStringAggregationTest[AllCommerceDBs, (sum: Boolean, avg: Boolean)] {
  def testDescription: String = "Aggregation: put aggregation in subexpression, stays as aggregation type"

  def query() =
    import AggregationExpr.toRow
    testDB.tables.products
      .aggregate(p =>
        (sum = sum(p.price)==1 , avg = avg(p.price) > p.price).toRow
      )

  def expectedQueryPattern: String =
    """SELECT SUM(product$A.price) = 1 as sum, AVG(product$A.price) > product$A.price as avg FROM product as product$A
        """
}

// TODO: multi-level expressions
class AggregateMultiSubexpression2AggregateTest extends SQLStringAggregationTest[AllCommerceDBs, (avg: Boolean)] {
  def testDescription: String = "Aggregation: put aggregation in subexpression, stays as aggregation type"

  def query() =
    import AggregationExpr.toRow
    testDB.tables.products
      .aggregate(p =>
        (avg = (avg(p.price) > p.price) == true).toRow
      )

  def expectedQueryPattern: String =
    """SELECT AVG(product$A.price) > product$A.price = "true" as avg FROM product as product$A
          """
}

// Query helper-method based aggregation:
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
    SELECT product$P.price > (SELECT SUM(product$B.price) FROM product as product$B) FROM product as product$P
      """
}