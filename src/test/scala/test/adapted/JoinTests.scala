package test
import tyql.*
import tyql.Expr.*
import language.experimental.namedTuples
import NamedTuple.*
// import scala.language.implicitConversions


import java.time.LocalDate

class JoinProjectTest extends SQLStringTest[AllCommerceDBs, (name: String, shippingDate: LocalDate)] {
  def testDescription = "Join: two-table simple join on int equality"
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
class JoinProjectTest2 extends SQLStringTest[AllCommerceDBs, (id: Int, id2: Int)] {
  def testDescription = "Join: two-table simple join, project ints"
  def query() =
    for
      b <- testDB.tables.buyers
      p <- testDB.tables.products
      if p.name == b.name
    yield (id = p.id, id2 = b.id).toRow

  def sqlString = """
        SELECT buyer0.name AS res_0, shipping_info1.shipping_date AS res_1
        FROM buyer buyer0
        JOIN shipping_info shipping_info1 ON (shipping_info1.buyer_id = buyer0.id)
      """
}
class Join3Test extends SQLStringTest[AllCommerceDBs, (buyerName: String, productName: String, price: Double)] {
  def testDescription = "Join: Compare with string literal"
  def query() =
    for
      b <- testDB.tables.buyers
      if b.name == "string constant"
      si <- testDB.tables.shipInfos
      pu <- testDB.tables.purchases
      pr <- testDB.tables.products
    yield (buyerName = b.name, productName = pr.name, price = pr.price).toRow

  def sqlString = """
        SELECT buyer0.name AS res_0, product3.name AS res_1, product3.price AS res_2
        FROM buyer buyer0
        JOIN shipping_info shipping_info1 ON (shipping_info1.id = buyer0.id)
        JOIN purchase purchase2 ON (purchase2.shipping_info_id = shipping_info1.id)
        JOIN product product3 ON (product3.id = purchase2.product_id)
        WHERE (buyer0.name = ?) AND (product3.price > ?)
      """
}

class Join4Test extends SQLStringTest[AllCommerceDBs, (buyerName: String, productName: String, price: Double)] {
  def testDescription = "Join: Multi compare, different guards"
  def query() =
    for
      b <- testDB.tables.buyers
      if b.name == "string constant"
      si <- testDB.tables.shipInfos
      if si.id == b.id
      pu <- testDB.tables.purchases
      pr <- testDB.tables.products
    yield (buyerName = b.name, productName = pr.name, price = pr.price).toRow

  def sqlString = """
        SELECT buyer0.name AS res_0, product3.name AS res_1, product3.price AS res_2
        FROM buyer buyer0
        JOIN shipping_info shipping_info1 ON (shipping_info1.id = buyer0.id)
        JOIN purchase purchase2 ON (purchase2.shipping_info_id = shipping_info1.id)
        JOIN product product3 ON (product3.id = purchase2.product_id)
        WHERE (buyer0.name = ?) AND (product3.price > ?)
      """
}

class Join5Test extends SQLStringTest[AllCommerceDBs, (buyerName: String, productName: String, price: Double)] {
  def testDescription = "Join: Multi compare, same guard"
  def query() =
    for
      b <- testDB.tables.buyers
      si <- testDB.tables.shipInfos
      pu <- testDB.tables.purchases
      pr <- testDB.tables.products
      if b.name == pr.name && si.id == b.id
    yield (buyerName = b.name, productName = pr.name, price = pr.price).toRow

  def sqlString = """
        SELECT buyer0.name AS res_0, product3.name AS res_1, product3.price AS res_2
        FROM buyer buyer0
        JOIN shipping_info shipping_info1 ON (shipping_info1.id = buyer0.id)
        JOIN purchase purchase2 ON (purchase2.shipping_info_id = shipping_info1.id)
        JOIN product product3 ON (product3.id = purchase2.product_id)
        WHERE (buyer0.name = ?) AND (product3.price > ?)
      """
}

class Join6Test extends SQLStringTest[AllCommerceDBs, (buyerName: String, productName: String, price: Double)] {
  def testDescription = "Join: Multi compare, same guard, string literal + compare"
  def query() =
    for
      b <- testDB.tables.buyers
      si <- testDB.tables.shipInfos
      pu <- testDB.tables.purchases
      pr <- testDB.tables.products
      if b.name == StringLit("string constant") && si.id == b.id
    yield (buyerName = b.name, productName = pr.name, price = pr.price).toRow

  def sqlString = """
        SELECT buyer0.name AS res_0, product3.name AS res_1, product3.price AS res_2
        FROM buyer buyer0
        JOIN shipping_info shipping_info1 ON (shipping_info1.id = buyer0.id)
        JOIN purchase purchase2 ON (purchase2.shipping_info_id = shipping_info1.id)
        JOIN product product3 ON (product3.id = purchase2.product_id)
        WHERE (buyer0.name = ?) AND (product3.price > ?)
      """
}

class Join7Test extends SQLStringTest[AllCommerceDBs, (buyerName: String, productName: String, price: Double)] {
  def testDescription = "Join: Multi compare, same guard, int literal + compare"
  def query() =
    for
      b <- testDB.tables.buyers
      si <- testDB.tables.shipInfos
      pu <- testDB.tables.purchases
      pr <- testDB.tables.products
      if b.id == IntLit(5) && si.id == b.id
    yield (buyerName = b.name, productName = pr.name, price = pr.price).toRow

  def sqlString = """
        SELECT buyer0.name AS res_0, product3.name AS res_1, product3.price AS res_2
        FROM buyer buyer0
        JOIN shipping_info shipping_info1 ON (shipping_info1.id = buyer0.id)
        JOIN purchase purchase2 ON (purchase2.shipping_info_id = shipping_info1.id)
        JOIN product product3 ON (product3.id = purchase2.product_id)
        WHERE (buyer0.name = ?) AND (product3.price > ?)
      """
}

// TODO: Join flavors, cross/left/etc
