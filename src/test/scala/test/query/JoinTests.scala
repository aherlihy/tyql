package test.query.join
import test.SQLStringTest
import test.query.{commerceDBs,  AllCommerceDBs}

import tyql.*
import tyql.Expr.*
import language.experimental.namedTuples
import NamedTuple.*
// import scala.language.implicitConversions


import java.time.LocalDate

class JoinSimple1Test extends SQLStringTest[AllCommerceDBs, (name: String, shippingDate: LocalDate)] {
  def testDescription = "Join: two-table simple join on int equality + project"
  def query() =
    // val q =
      for
        b <- testDB.tables.buyers
        si <- testDB.tables.shipInfos
        if si.buyerId == b.id
      yield (name = b.name, shippingDate = si.shippingDate).toRow
    //q

  def sqlString = """
        SELECT buyer0.name AS res_0, shipping_info1.shipping_date AS res_1
        FROM buyer buyer0
        JOIN shipping_info shipping_info1 ON (shipping_info1.buyer_id = buyer0.id)
      """
}
class JoinSimple2Test extends SQLStringTest[AllCommerceDBs, (id: Int, id2: Int)] {
  def testDescription = "Join: two-table simple join on string equality + project"
  def query() =
    for
      b <- testDB.tables.buyers
      p <- testDB.tables.products
      if p.name == b.name
    yield (id = p.id, id2 = b.id).toRow

  def sqlString = """
      """
}
class JoinSimple3Test extends SQLStringTest[AllCommerceDBs, (buyerName: String, productName: String, price: Double)] {
  def testDescription = "Join: two-table simple join on string literal comparison"
  def query() =
    for
      b <- testDB.tables.buyers
      if b.name == "string constant"
      pr <- testDB.tables.products
    yield (buyerName = b.name, productName = pr.name, price = pr.price).toRow

  def sqlString = """
      """
}

class JoinSimple4Test extends SQLStringTest[AllCommerceDBs, (buyerName: String, productName: String, price: Double)] {
  def testDescription = "Join: two-table simple join with separate conditions"
  def query() =
    for
      b <- testDB.tables.buyers
      if b.name == "string constant"
      pr <- testDB.tables.products
      if pr.id == b.id
    yield (buyerName = b.name, productName = pr.name, price = pr.price).toRow

  def sqlString = """
      """
}

class JoinSimple5Test extends SQLStringTest[AllCommerceDBs, (buyerName: String, productName: String, price: Double)] {
  def testDescription = "Join: two-table simple join with &&"
  def query() =
    for
      b <- testDB.tables.buyers
      pr <- testDB.tables.products
      if b.name == pr.name && pr.id == b.id
    yield (buyerName = b.name, productName = pr.name, price = pr.price).toRow

  def sqlString = """
      """
}

class JoinSimple6Test extends SQLStringTest[AllCommerceDBs, (buyerName: String, productName: String, price: Double)] {
  def testDescription = "Join: two-table simple join with && and string literal"
  def query() =
    for
      b <- testDB.tables.buyers
      pr <- testDB.tables.products
      if b.name == StringLit("string constant") && pr.id == b.id
    yield (buyerName = b.name, productName = pr.name, price = pr.price).toRow

  def sqlString = """
      """
}

class JoinSimple7Test extends SQLStringTest[AllCommerceDBs, (buyerName: String, productName: String, price: Double)] {
  def testDescription = "Join: two-table simple join with && and int literal"
  def query() =
    for
      b <- testDB.tables.buyers
      pr <- testDB.tables.products
      if b.id == IntLit(5) && pr.id == b.id
    yield (buyerName = b.name, productName = pr.name, price = pr.price).toRow

  def sqlString = """
      """
}

// TODO: Join flavors, cross/left/etc
