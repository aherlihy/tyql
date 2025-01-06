package test.query.join
import test.SQLStringQueryTest
import test.query.{commerceDBs, AllCommerceDBs}

import tyql.*
import tyql.Expr.*
import language.experimental.namedTuples
import NamedTuple.*
// import scala.language.implicitConversions

import tyql.Dialect.ansi.given

import java.time.LocalDate

class JoinSimple1Test extends SQLStringQueryTest[AllCommerceDBs, (name: String, shippingDate: LocalDate)] {
  def testDescription = "Join: two-table simple join on int equality + project"
  def query() =
    // val q =
    for
      b <- testDB.tables.buyers
      si <- testDB.tables.shipInfos
      if si.buyerId == b.id
    yield (name = b.name, shippingDate = si.shippingDate).toRow
//  q

  def expectedQueryPattern: String = """
        SELECT buyers$A.name as name, shippingInfo$B.shippingDate as shippingDate
        FROM buyers as buyers$A, shippingInfo as shippingInfo$B
        WHERE shippingInfo$B.buyerId = buyers$A.id
      """
}

class JoinSimple2Test extends SQLStringQueryTest[AllCommerceDBs, (newId: Int, newId2: Int)] {
  def testDescription = "Join: two-table simple join on string equality + project"
  def query() =
    for
      b <- testDB.tables.buyers
      p <- testDB.tables.products
      if p.name == b.name
    yield (newId = p.id, newId2 = b.id).toRow

  def expectedQueryPattern: String = """
  SELECT product$A.id as newId, buyers$B.id as newId2 FROM buyers as buyers$B, product as product$A WHERE product$A.name = buyers$B.name
  """
}
class JoinSimple3Test
    extends SQLStringQueryTest[AllCommerceDBs, (buyerName: String, productName: String, price: Double)] {
  def testDescription = "Join: two-table simple join on string literal comparison"
  def query() =
    for
      b <- testDB.tables.buyers
      if b.name == "string constant"
      pr <- testDB.tables.products
    yield (buyerName = b.name, productName = pr.name, price = pr.price).toRow

  def expectedQueryPattern: String = """
      SELECT
        buyers$B.name as buyerName, product$P.name as productName, product$P.price as price
      FROM
        buyers as buyers$B,
        product as product$P
      WHERE buyers$B.name = 'string constant'
      """
}

class JoinSimple4Test
    extends SQLStringQueryTest[AllCommerceDBs, (buyerName: String, productName: String, price: Double)] {
  def testDescription = "Join: two-table simple join with separate conditions"
  def query() =
    for
      b <- testDB.tables.buyers
      if b.name == "string constant"
      pr <- testDB.tables.products
      if pr.id == b.id
    yield (buyerName = b.name, productName = pr.name, price = pr.price).toRow

  def expectedQueryPattern: String = """
SELECT buyers$A.name as buyerName, product$B.name as productName, product$B.price as price
FROM buyers as buyers$A, product as product$B
WHERE (buyers$A.name = 'string constant' AND product$B.id = buyers$A.id)
      """
}

class JoinSimple5Test
    extends SQLStringQueryTest[AllCommerceDBs, (buyerName: String, productName: String, price: Double)] {
  def testDescription = "Join: two-table simple join with &&"
  def query() =
    for
      b <- testDB.tables.buyers
      pr <- testDB.tables.products.joinOn(pr => b.name == pr.name && pr.id == b.id)
    yield (buyerName = b.name, productName = pr.name, price = pr.price).toRow

  def expectedQueryPattern: String = """
SELECT buyers$A.name as buyerName, product$B.name as productName, product$B.price as price
FROM buyers as buyers$A
JOIN product as product$B ON buyers$A.name = product$B.name AND product$B.id = buyers$A.id
     """
}

//class JoinSimple6Test extends SQLStringQueryTest[AllCommerceDBs, (buyerName: String, productName: String, price: Double)] {
//  def testDescription = "Join: two-table simple join with && and string literal"
//  def query() =
//    for
//      b <- testDB.tables.buyers
//      pr <- testDB.tables.products
//      if b.name == StringLit("string constant") && pr.id == b.id
//    yield (buyerName = b.name, productName = pr.name, price = pr.price).toRow
//
//  def expectedQueryPattern: String = """
//  SELECT b.name AS buyerName, p.name AS productName, p.price
//FROM buyers b
//JOIN products p ON (b.name = 'string constant' AND p.id = b.id)
//      """
//}
//
//class JoinSimple7Test extends SQLStringQueryTest[AllCommerceDBs, (buyerName: String, productName: String, price: Double)] {
//  def testDescription = "Join: two-table simple join with && and int literal"
//  def query() =
//    for
//      b <- testDB.tables.buyers
//      pr <- testDB.tables.products
//      if b.id == IntLit(5) && pr.id == b.id
//    yield (buyerName = b.name, productName = pr.name, price = pr.price).toRow
//
//  def expectedQueryPattern: String = """
//  SELECT b.name AS buyerName, p.name AS productName, p.price
//FROM buyers b
//JOIN products p ON (b.id = 5 AND p.id = b.id)
//      """
//}
//
//class FlatJoin2Test extends SQLStringQueryTest[AllCommerceDBs, (name: String, shippingDate: LocalDate)] {
//  def testDescription = "Join: simple flatmap"
//  def query() =
//    for {
//      b <- testDB.tables.buyers
//      si <- testDB.tables.shipInfos
//      if si.buyerId == b.id
//    } yield (name = b.name, shippingDate = si.shippingDate).toRow
//
//  def expectedQueryPattern: String = """
//        SELECT buyer0.name, shipping_info1.shipping_date
//        FROM buyer buyer0
//        JOIN shipping_info shipping_info1 ON (shipping_info1.buyer_id = buyer0.id)
//      """
//}
//class FlatJoin3Test extends SQLStringQueryTest[AllCommerceDBs, (buyerName: String, productName: String, price: Double)] {
//  def testDescription = "Join: flat join 3"
//  def query() =
//    for {
//      b <- testDB.tables.buyers
//      if b.name == "name"
//      si <- testDB.tables.shipInfos
//      if si.buyerId == b.id
//      pu <- testDB.tables.purchases
//      if pu.shippingInfoId == si.id
//      pr <- testDB.tables.products
//      if pr.id == pu.productId && pr.price > 1.0
//    } yield
//      (buyerName = b.name, productName = pr.name, price = pr.price).toRow
//
//  def expectedQueryPattern: String = """
//        SELECT buyer0.name AS res_0, product3.name AS res_1, product3.price AS res_2
//        FROM buyer buyer0
//        JOIN shipping_info shipping_info1 ON (shipping_info1.id = buyer0.id)
//        JOIN purchase purchase2 ON (purchase2.shipping_info_id = shipping_info1.id)
//        JOIN product product3 ON (product3.id = purchase2.product_id)
//        WHERE (buyer0.name = 'name') AND (product3.price > 1.0)
//      """
//}
//
//class LeftJoinFlatJoinTest extends SQLStringQueryTest[AllCommerceDBs, (buyerName: String, shippingDate: LocalDate)] {
//  def testDescription = "Join: leftJoin"
//  def query() =
//    for {
//      b <- testDB.tables.buyers
//      si <- testDB.tables.shipInfos
//      if si.buyerId == b.id // TODO: specify left join specifically?
//    } yield (buyerName = b.name, shippingDate = si.shippingDate).toRow
//
//  def expectedQueryPattern: String = """
//        SELECT buyer0.name, shipping_info1.shipping_date
//        FROM buyer buyer0
//        LEFT JOIN shipping_info shipping_info1 ON (shipping_info1.buyer_id = buyer0.id)
//      """
//}
//class FlatJoin4Test extends SQLStringQueryTest[AllCommerceDBs, LocalDate] {
//  def testDescription = "Join: flat join 4"
//  def query() =
//    testDB.tables.buyers.flatMap(b =>
//        testDB.tables.shipInfos.map(s =>
//          (buyer = b, shipInfo = s).toRow
//        )
//      )
//      .filter(tup => tup.buyer.id == tup.shipInfo.buyerId && tup.buyer.name == "name")
//      .map(t => t.shipInfo.shippingDate)
//
//  def expectedQueryPattern: String = """
//        SELECT shipping_info1.shipping_date AS res
//        FROM buyer buyer0
//        CROSS JOIN shipping_info shipping_info1
//        WHERE ((buyer0.id = shipping_info1.buyer_id) AND (buyer0.name = 'name'))
//      """
//}
//
//class FlatJoin5Test extends SQLStringQueryTest[AllCommerceDBs, LocalDate] {
//  def testDescription = "Join: flat join 5"
//  def query() =
//    for {
//      b <- testDB.tables.buyers
//      s <- testDB.tables.shipInfos
//      if b.id == s.buyerId && b.name == "name"
//    } yield s.shippingDate
//
//  def expectedQueryPattern: String = """
//        SELECT shipping_info1.shipping_date AS res
//        FROM buyer buyer0
//        CROSS JOIN shipping_info shipping_info1
//        WHERE ((buyer0.id = shipping_info1.buyer_id) AND (buyer0.name = 'name'))
//      """
//}
//
//class FlatJoin6Test extends SQLStringQueryTest[AllCommerceDBs, LocalDate] {
//  def testDescription = "Join: flat join 6"
//  def query() =
//    for {
//      b <- testDB.tables.buyers.filter(_.name == "name")
//      s <- testDB.tables.shipInfos
//      if b.id == s.buyerId
//    } yield s.shippingDate
//
//  def expectedQueryPattern: String = """
//        SELECT shipping_info1.shipping_date AS res
//        FROM buyer buyer0
//        CROSS JOIN shipping_info shipping_info1
//        WHERE (buyer0.name = 'name') AND (buyer0.id = shipping_info1.buyer_id)
//      """
//}
//
//class flatMapForCompoundFlatJoinTest extends SQLStringQueryTest[AllCommerceDBs, (name: String, date: LocalDate)] {
//  def testDescription = "Join: flatMapForCompound"
//  def query() =
//    for {
//      b <- testDB.tables.buyers.sort(_.id, Ord.ASC).take(1)
//      si <- testDB.tables.shipInfos.sort(_.id, Ord.ASC).take(1)
//    } yield (name = b.name, date = si.shippingDate).toRow
//
//  def expectedQueryPattern: String = """
//        SELECT
//          subquery0.name,
//          subquery1.shipping_date
//        FROM
//          (SELECT buyer0.id AS id, buyer0.name AS name
//          FROM buyer buyer0
//          ORDER BY id ASC
//          LIMIT 1) subquery0
//        CROSS JOIN (SELECT
//            shipping_info1.id AS id,
//            shipping_info1.shipping_date AS shipping_date
//          FROM shipping_info shipping_info1
//          ORDER BY id ASC
//          LIMIT 1) subquery1
//      """
//}
