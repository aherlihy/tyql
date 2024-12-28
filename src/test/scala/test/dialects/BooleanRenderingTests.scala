package test.integration.booleanliterals

import test.TestDatabase
import test.SQLStringQueryTest
import test.query.{AllCommerceDBs, ShippingInfo, commerceDBs, Buyer}
import tyql.*
import language.experimental.namedTuples
import NamedTuple.*
import scala.language.implicitConversions

class BooleanTrueRenderingTests extends SQLStringQueryTest[AllCommerceDBs, String] {
  override def testDescription: String = "Just LIMIT remains uniform across all dialects"
  def query() = testDB.tables.products.filter{ _ => tyql.Expr.BooleanLit(true) }.map( c => c.name)
  def expectedQueryPattern = """SELECT product$A.name FROM product as product$A WHERE TRUE"""
}

class BooleanFalseRenderingTests extends SQLStringQueryTest[AllCommerceDBs, String] {
  override def testDescription: String = "Just LIMIT remains uniform across all dialects"
  def query() = testDB.tables.products.filter{ _ => tyql.Expr.BooleanLit(false) }.map( c => c.name)
  def expectedQueryPattern = """SELECT product$A.name FROM product as product$A WHERE FALSE"""
}

// TODO is there are simpler way of doing `select true`?
// TODO why do I have to do tyql.Expr.BooleanLit(false) ?
