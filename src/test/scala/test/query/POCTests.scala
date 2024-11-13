package test.query.poc
// tests that illustrate a particular claim / proof-of-concept

import test.{SQLStringQueryTest, TestDatabase}
import test.query.{commerceDBs, AllCommerceDBs, Product}

import tyql.*
import language.experimental.namedTuples
import NamedTuple.*

class AbstractOverValuesHostTest extends SQLStringQueryTest[AllCommerceDBs, Product] {
  def testDescription: String = "Support for abstracting over values in the host language"
  def query() =
    val range = (a: Double, b: Double) =>
      testDB.tables.products
        .filter(p => p.price > a && p.price < b)

    range(5, 10)
  def expectedQueryPattern: String =
    "SELECT * FROM product as product$A WHERE product$A.price > 5.0 AND product$A.price < 10.0"
}

class AbstractOverPredicatesHostTest extends SQLStringQueryTest[AllCommerceDBs, Product] {
  def testDescription: String = "Support for abstracting over predicates in the host language"
  def query() =
    val satisfies: Expr[Double, NonScalarExpr] => Expr[Boolean, NonScalarExpr] = (p => p > 10.0)
    testDB.tables.products.filter(p => satisfies(p.price))
  def expectedQueryPattern: String = "SELECT * FROM product as product$A WHERE product$A.price > 10.0"
}

class AbstractOverValuesAndPredicatesHostTest extends SQLStringQueryTest[AllCommerceDBs, Product] {
  def testDescription: String = "Support for abstracting over predicates and values in the host language"
  def query() =
    val satisfies: (Expr[Double, NonScalarExpr], Double) => Expr[Boolean, NonScalarExpr] = (p, q) => p > q

    val range = (a: Double, b: Double) =>
      testDB.tables.products
        .filter(p => satisfies(p.price, a) && p.price < b)

    range(10, 5)
  def expectedQueryPattern: String =
    "SELECT * FROM product as product$A WHERE product$A.price > 10.0 AND product$A.price < 5.0"
}

// TODO: DSL-substitute not implemented
//class AbstractOverValuesEmbeddedTest extends SQLStringQueryTest[AllCommerceDBs, Product] {
//  def testDescription: String = "Support for abstracting over values in the embedded language"
//  def query() =
//    val param = Expr.RefExpr[Double, NonScalarExpr]()
//    val body = Expr.GtDouble(param, Expr.DoubleLit(5))
//    val range = Expr.AbstractedExpr(param, body)
//
//    testDB.tables.products.filter(p => range.apply(p.price))
//  def expectedQueryPattern: String = "SELECT * FROM product as product$A WHERE product$A.price > 5.0 AND product$A.price < 10.0"
//}
