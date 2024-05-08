package test
import tyql.*
import language.experimental.namedTuples
import NamedTuple.*
import scala.language.implicitConversions


import java.time.LocalDate

class JoinTest extends SQLStringTest[AllCommerceDBs, (name: String, shippingDate: LocalDate)] {
  def testDescription = "two-table simple join"
  def query() =
    val q =
      for
        b <- testDB.tables.buyers
        si <- testDB.tables.shipInfos
        if si.buyerId == b.id
      yield (name = b.name, shippingDate = si.shippingDate)
    q

  def sqlString = """
        SELECT buyer0.name AS res_0, shipping_info1.shipping_date AS res_1
        FROM buyer buyer0
        JOIN shipping_info shipping_info1 ON (shipping_info1.buyer_id = buyer0.id)
      """
}
class Join2Test extends SQLStringTest[AllCommerceDBs, (buyerName: String, productName: String, price: Double)] {
  def testDescription = "Compare with string"
  def query() =
    val q =
      for
        b <- testDB.tables.buyers
        // if b.name == "Li Haoyi"
        si <- testDB.tables.shipInfos
        // if si.id == b.id
        pu <- testDB.tables.purchases
        // if pu.shippingInfoId == si.id
        pr <- testDB.tables.products
        // if pr.id == pu.productId
        if /*b.name == "Li Haoyi" && */ si.buyerId == b.id && pu.shippingInfoId == si.id// && pr.id == pu.productId
      yield (buyerName = b.name, productName = pr.name, price = pr.price)
    q

  def sqlString = """
        SELECT buyer0.name AS res_0, product3.name AS res_1, product3.price AS res_2
        FROM buyer buyer0
        JOIN shipping_info shipping_info1 ON (shipping_info1.id = buyer0.id)
        JOIN purchase purchase2 ON (purchase2.shipping_info_id = shipping_info1.id)
        JOIN product product3 ON (product3.id = purchase2.product_id)
        WHERE (buyer0.name = ?) AND (product3.price > ?)
      """
}

// class LeftJoinTest extends SQLStringTest[(Buyer, ShippingInfo), String] {
//   def query() =
//     for {
//       b <- testDB.tables.buyers
//       si <- ShippingInfo.leftJoin(_.buyerId `=` b.id)
//     } yield (b.name, si.map(_.shippingDate))
//   def sqlString = """
//         SELECT buyer0.name AS res_0, shipping_info1.shipping_date AS res_1
//         FROM buyer buyer0
//         LEFT JOIN shipping_info shipping_info1 ON (shipping_info1.buyer_id = buyer0.id)
//       """
// }
// class FlatMapTest extends SQLStringTest[(Buyer, ShippingInfo), String] {
//   def query() =
//     testDB.tables.buyers
//       .flatMap(b => ShippingInfo.crossJoin().map((b, _)))
//       .filter { case (b, s) => b.id `=` s.buyerId && b.name `=` "James Bond" }
//       .map(_._2.shippingDate)
//   def sqlString = """
//         SELECT shipping_info1.shipping_date AS res
//         FROM buyer buyer0
//         CROSS JOIN shipping_info shipping_info1
//         WHERE ((buyer0.id = shipping_info1.buyer_id) AND (buyer0.name = ?))
//       """
// }

// class FlatMapForTest extends SQLStringTest[(Buyer, ShippingInfo), String] {
//   def query() =
//     for {
//       b <- testDB.tables.buyers
//       s <- ShippingInfo.crossJoin()
//       if b.id `=` s.buyerId && b.name `=` "James Bond"
//     } yield s.shippingDate
//   def sqlString = """
//         SELECT shipping_info1.shipping_date AS res
//         FROM buyer buyer0
//         CROSS JOIN shipping_info shipping_info1
//         WHERE ((buyer0.id = shipping_info1.buyer_id) AND (buyer0.name = ?))
//       """
// }

// class FlatMapForFilterTest extends SQLStringTest[(Buyer, ShippingInfo), String] {
//   def query() =
//     for {
//       b <- testDB.tables.buyers.filter(_.name `=` "James Bond")
//       s <- ShippingInfo.crossJoin().filter(b.id `=` _.buyerId)
//     } yield s.shippingDate
//   def sqlString = """
//         SELECT shipping_info1.shipping_date AS res
//         FROM buyer buyer0
//         CROSS JOIN shipping_info shipping_info1
//         WHERE (buyer0.name = ?) AND (buyer0.id = shipping_info1.buyer_id)
//       """
// }

// class FlatMapForJoinTest extends SQLStringTest[(Product, Buyer, ShippingInfo, Purchase), String] {
//   def query() =
//     for {
//       (b, si) <- testDB.tables.buyers.join(ShippingInfo)(_.id `=` _.buyerId)
//       (pu, pr) <- Purchase.select.join(Product)(_.productId `=` _.id).crossJoin()
//       if si.id `=` pu.shippingInfoId
//     } yield (b.name, pr.name)
//   def sqlString = """
//         SELECT buyer0.name AS res_0, subquery2.res_1_name AS res_1
//         FROM buyer buyer0
//         JOIN shipping_info shipping_info1 ON (buyer0.id = shipping_info1.buyer_id)
//         CROSS JOIN (SELECT
//             purchase2.shipping_info_id AS res_0_shipping_info_id,
//             product3.name AS res_1_name
//           FROM purchase purchase2
//           JOIN product product3 ON (purchase2.product_id = product3.id)) subquery2
//         WHERE (shipping_info1.id = subquery2.res_0_shipping_info_id)
//       """
// }
// class FlatMapForGroupByTest extends SQLStringTest[(Buyer, ShippingInfo), String] {
//   def query() =
//     for {
//       (name, dateOfBirth) <- testDB.tables.buyers.groupBy(_.name)(_.minBy(_.dateOfBirth))
//       shippingInfo <- ShippingInfo.crossJoin()
//     } yield (name, dateOfBirth, shippingInfo.id, shippingInfo.shippingDate)

//   def sqlString =
//     """
//         SELECT
//           subquery0.res_0 AS res_0,
//           subquery0.res_1 AS res_1,
//           shipping_info1.id AS res_2,
//           shipping_info1.shipping_date AS res_3
//         FROM (SELECT buyer0.name AS res_0, MIN(buyer0.date_of_birth) AS res_1
//           FROM buyer buyer0
//           GROUP BY buyer0.name) subquery0
//         CROSS JOIN shipping_info shipping_info1
//       """
// }
// class FlatMapForGroupBy2Test extends SQLStringTest[(Buyer, ShippingInfo), String] {
//   def query() =
//     for {
//       (name, dateOfBirth) <- testDB.tables.buyers.groupBy(_.name)(_.minBy(_.dateOfBirth))
//       (shippingInfoId, shippingDate) <- ShippingInfo.select
//         .groupBy(_.id)(_.minBy(_.shippingDate))
//         .crossJoin()
//     } yield (name, dateOfBirth, shippingInfoId, shippingDate)

//   def sqlString =
//     """
//         SELECT
//           subquery0.res_0 AS res_0,
//           subquery0.res_1 AS res_1,
//           subquery1.res_0 AS res_2,
//           subquery1.res_1 AS res_3
//         FROM (SELECT
//             buyer0.name AS res_0,
//             MIN(buyer0.date_of_birth) AS res_1
//           FROM buyer buyer0
//           GROUP BY buyer0.name) subquery0
//         CROSS JOIN (SELECT
//             shipping_info1.id AS res_0,
//             MIN(shipping_info1.shipping_date) AS res_1
//           FROM shipping_info shipping_info1
//           GROUP BY shipping_info1.id) subquery1
//       """
// }
// class FlatMapForCompoundTest extends SQLStringTest[(Buyer, ShippingInfo), String] {
//   def query() =
//     for {
//       b <- testDB.tables.buyers.sortBy(_.id).asc.take(1)
//       si <- ShippingInfo.select.sortBy(_.id).asc.take(1).crossJoin()
//     } yield (b.name, si.shippingDate)
//   def sqlString = """
//         SELECT
//           subquery0.name AS res_0,
//           subquery1.shipping_date AS res_1
//         FROM
//           (SELECT buyer0.id AS id, buyer0.name AS name
//           FROM buyer buyer0
//           ORDER BY id ASC
//           LIMIT ?) subquery0
//         CROSS JOIN (SELECT
//             shipping_info1.id AS id,
//             shipping_info1.shipping_date AS shipping_date
//           FROM shipping_info shipping_info1
//           ORDER BY id ASC
//           LIMIT ?) subquery1
//       """
// }
