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
        SELECT buyer0.name, shipping_info1.shipping_date
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
class FlatJoin2Test extends SQLStringTest[AllCommerceDBs, (name: String, shippingDate: LocalDate)] {
  def testDescription = "Join: simple flatmap"
  def query() =
        for {
          b <- testDB.tables.buyers
          si <- testDB.tables.shipInfos
          if si.buyerId == b.id
        } yield (name = b.name, shippingDate = si.shippingDate).toRow

  def sqlString = """
        SELECT buyer0.name, shipping_info1.shipping_date
        FROM buyer buyer0
        JOIN shipping_info shipping_info1 ON (shipping_info1.buyer_id = buyer0.id)
      """
}
class FlatJoin3Test extends SQLStringTest[AllCommerceDBs, (buyerName: String, productName: String, price: Double)] {
  def testDescription = "Join: flat join 3"
  def query() =
        for {
          b <- testDB.tables.buyers
          if b.name == "name"
          si <- testDB.tables.shipInfos
          if si.buyerId == b.id
          pu <- testDB.tables.purchases
          if pu.shippingInfoId == si.id
          pr <- testDB.tables.products
          if pr.id == pu.productId && pr.price > 1.0
        } yield
          (buyerName = b.name, productName = pr.name, price = pr.price).toRow

  def sqlString = """
        SELECT buyer0.name AS res_0, product3.name AS res_1, product3.price AS res_2
        FROM buyer buyer0
        JOIN shipping_info shipping_info1 ON (shipping_info1.id = buyer0.id)
        JOIN purchase purchase2 ON (purchase2.shipping_info_id = shipping_info1.id)
        JOIN product product3 ON (product3.id = purchase2.product_id)
        WHERE (buyer0.name = ?) AND (product3.price > ?)
      """
}

class LeftJoinFlatJoinTest extends SQLStringTest[AllCommerceDBs, (buyerName: String, shippingDate: LocalDate)] {
  def testDescription = "Join: leftJoin"
  def query() =
        for {
          b <- testDB.tables.buyers
          si <- testDB.tables.shipInfos
          if si.buyerId == b.id // TODO: specify left join specifically?
        } yield (buyerName = b.name, shippingDate = si.shippingDate).toRow

  def sqlString = """
        SELECT buyer0.name, shipping_info1.shipping_date
        FROM buyer buyer0
        LEFT JOIN shipping_info shipping_info1 ON (shipping_info1.buyer_id = buyer0.id)
      """
}
class FlatJoin4Test extends SQLStringTest[AllCommerceDBs, LocalDate] {
  def testDescription = "Join: flat join 4"
  def query() =
        testDB.tables.buyers.flatMap(b =>
          testDB.tables.shipInfos.map(s =>
            (buyer = b, shipInfo = s)
          )
        )
        .filter(tup => tup.buyer.id == tup.shipInfo.buyerId && tup.buyer.name == "name")
        .map(t => t.shipInfo.shippingDate)

  def sqlString = """
        SELECT shipping_info1.shipping_date AS res
        FROM buyer buyer0
        CROSS JOIN shipping_info shipping_info1
        WHERE ((buyer0.id = shipping_info1.buyer_id) AND (buyer0.name = ?))
      """
}

class FlatJoin5Test extends SQLStringTest[AllCommerceDBs, LocalDate] {
  def testDescription = "Join: flat join 5"
  def query() =
        for {
          b <- testDB.tables.buyers
          s <- testDB.tables.shipInfos
          if b.id == s.buyerId && b.name == "name"
        } yield s.shippingDate

  def sqlString = """
        SELECT shipping_info1.shipping_date AS res
        FROM buyer buyer0
        CROSS JOIN shipping_info shipping_info1
        WHERE ((buyer0.id = shipping_info1.buyer_id) AND (buyer0.name = ?))
      """
}

class FlatJoin6Test extends SQLStringTest[AllCommerceDBs, LocalDate] {
  def testDescription = "Join: flat join 6"
  def query() =
        for {
          b <- testDB.tables.buyers.filter(_.name == "name")
          s <- testDB.tables.shipInfos
          if b.id == s.buyerId
        } yield s.shippingDate

  def sqlString = """
        SELECT shipping_info1.shipping_date AS res
        FROM buyer buyer0
        CROSS JOIN shipping_info shipping_info1
        WHERE (buyer0.name = ?) AND (buyer0.id = shipping_info1.buyer_id)
      """
}

class FlatJoin7Test extends SQLStringTest[AllCommerceDBs, (buyerName: String, productName: String)] {
  def testDescription = "Join: flat join 7"
  def query() =
        for {
          t1 <- testDB.tables.buyers.flatMap(b => testDB.tables.shipInfos.map(si => (buy = b, shi = si).toRow)).filter(t => (t.buy.id ==t.shi.buyerId))
          t2 <- testDB.tables.purchases.flatMap(pu => testDB.tables.products.map(pr => (pur = pu, pro = pr).toRow)).filter(t => (t.pur.productId ==t.pro.id))
          if t1.shi.id == t2.pur.shippingInfoId
        } yield (buyerName = t1.buy.name, productName = t2.pro.name).toRow

  def sqlString = """
        SELECT buyer0.name, subquery2.res_1_name
        FROM buyer buyer0
        JOIN shipping_info shipping_info1 ON (buyer0.id = shipping_info1.buyer_id)
        CROSS JOIN (SELECT
            testDB.tables.purchases2.shipping_info_id_shipping_info_id,
            product3.name_name
          FROM testDB.tables.purchases testDB.tables.purchases2
          JOIN product product3 ON (testDB.tables.purchases2.product_id = product3.id)) subquery2
        WHERE (shipping_info1.id = subquery2.res_0_shipping_info_id)
      """
}
/*
class flatMapForGroupByFlatJoinTest extends SQLStringTest[AllCommerceDBs, (buyerName: String, productName: String, price: Double)] {
  def testDescription = "Join: flatMapForGroupBy"
  def query() =
        for {
          (name, dateOfBirth) <- testDB.tables.buyers.groupBy(_.name)(_.minBy(_.dateOfBirth))
          testDB.tables.shipInfos <- testDB.tables.shipInfos.crossJoin()
        } yield (name, dateOfBirth, testDB.tables.shipInfos.id, testDB.tables.shipInfos.shippingDate)

  def sqlString = """
        SELECT
          subquery0.res_0,
          subquery0.res_1,
          shipping_info1.id AS res_2,
          shipping_info1.shipping_date AS res_3
        FROM (SELECT buyer0.name, MIN(buyer0.date_of_birth)
          FROM buyer buyer0
          GROUP BY buyer0.name) subquery0
        CROSS JOIN shipping_info shipping_info1
      """
}
class flatMapForGroupBy2FlatJoinTest extends SQLStringTest[AllCommerceDBs, (buyerName: String, productName: String, price: Double)] {
  def testDescription = "Join: flatMapForGroupBy2"
  def query() =
        for {
          (name, dateOfBirth) <- testDB.tables.buyers.groupBy(_.name)(_.minBy(_.dateOfBirth))
          (testDB.tables.shipInfosId, shippingDate) <- testDB.tables.shipInfos.select
            .groupBy(_.id)(_.minBy(_.shippingDate))
            .crossJoin()
        } yield (name, dateOfBirth, testDB.tables.shipInfosId, shippingDate)

  def sqlString = """
        SELECT
          subquery0.res_0,
          subquery0.res_1,
          subquery1.res_0 AS res_2,
          subquery1.res_1 AS res_3
        FROM (SELECT
            buyer0.name,
            MIN(buyer0.date_of_birth)
          FROM buyer buyer0
          GROUP BY buyer0.name) subquery0
        CROSS JOIN (SELECT
            shipping_info1.id,
            MIN(shipping_info1.shipping_date)
          FROM shipping_info shipping_info1
          GROUP BY shipping_info1.id) subquery1
      """
}
 */
//class flatMapForCompoundFlatJoinTest extends SQLStringTest[AllCommerceDBs, (buyerName: String, productName: String, price: Double)] {
//  def testDescription = "Join: flatMapForCompound"
//  def query() =
//        for {
//          b <- testDB.tables.buyers.sortBy(_.id).take(1)
//          si <- testDB.tables.shipInfos.sortBy(_.id)(1)
//        } yield (b.name, si.shippingDate)
//
//  def sqlString = """
//        SELECT
//          subquery0.name,
//          subquery1.shipping_date
//        FROM
//          (SELECT buyer0.id AS id, buyer0.name AS name
//          FROM buyer buyer0
//          ORDER BY id ASC
//          LIMIT ?) subquery0
//        CROSS JOIN (SELECT
//            shipping_info1.id AS id,
//            shipping_info1.shipping_date AS shipping_date
//          FROM shipping_info shipping_info1
//          ORDER BY id ASC
//          LIMIT ?) subquery1
//      """
//  }
