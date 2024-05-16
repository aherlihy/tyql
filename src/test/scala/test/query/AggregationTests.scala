package test.query.selectmodifiers
import test.SQLStringTest
import test.query.{commerceDBs,  AllCommerceDBs}

import tyql.*
import tyql.Expr.toRow
import language.experimental.namedTuples
import NamedTuple.*
import scala.language.implicitConversions

/*
class SelectModifiersSumSelectTest extends SQLStringTest[AllCommerceDBs, Double] {
  def testDescription: String = "SelectModifiers: sum"
  def query() =
    testDB.tables.products.sum(_.price)
  def sqlString: String = "SELECT SUM(product.price) FROM product"
}

class SelectModifiersSum2SelectTest extends SQLStringTest[AllCommerceDBs, Double] {
  def testDescription: String = "SelectModifiers: sum with filter"
  def query() =
    testDB.tables.products.withFilter(p => p.price != 0).sum(_.price)
  def sqlString: String = "SELECT SUM(product.price) FROM product WHERE product.price > 0"
}

class SelectModifiersSum3SelectTest extends SQLStringTest[AllCommerceDBs, Double] {
  def testDescription: String = "SelectModifiers: sum with map"
  def query() =
    testDB.tables.products.withFilter(p => p.price != 0).sum(_.price)
  def sqlString: String = "SELECT SUM(product.price) FROM product WHERE product.price > 0"
}

class SelectModifiersSum4SelectTest extends SQLStringTest[AllCommerceDBs, Double] {
  def testDescription: String = "SelectModifiers: sum with map"
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
*/

class SelectModifiersMultiAggregateTest extends SQLStringTest[AllCommerceDBs, (sum: Double, avg: Double)] {
  def testDescription: String = "SelectModifiers: aggregate"
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