package test.query.subquery

import test.SQLStringTest
import test.query.{AllCommerceDBs, Buyer, commerceDBs}
import tyql.*
import tyql.Expr.*

import language.experimental.namedTuples
import NamedTuple.*
// import scala.language.implicitConversions

import java.time.LocalDate

class sortTakeJoinSubqueryTest extends SQLStringTest[AllCommerceDBs, Double] {
  def testDescription = "Subquery: sortTakeJoin"
  def query() =
    testDB.tables.purchases.flatMap(purch =>
      testDB.tables.products.sort(_.price, Ord.DESC).take(1)
        .filter(prod => prod.id == purch.id)
        .map(prod => purch.total)
    )
  def sqlString = """
        SELECT purchase0.total AS res
        FROM purchase purchase0
        JOIN (SELECT product1.id AS id, product1.price AS price
          FROM product product1
          ORDER BY price DESC
          LIMIT ?) subquery1
        ON (purchase0.product_id = subquery1.id)
      """
}

class sortTake2JoinSubqueryTest extends SQLStringTest[AllCommerceDBs, Double] {
  def testDescription = "Subquery: sortTakeJoin (for comprehension)"
  def query() =
    testDB.tables.purchases.flatMap(purch =>
      testDB.tables.products.sort(_.price, Ord.DESC).take(1)
        .filter(prod => prod.id == purch.id)
        .map(prod => purch.total)
    )

  def sqlString =
    """
          SELECT purchase0.total AS res
          FROM purchase purchase0
          JOIN (SELECT product1.id AS id, product1.price AS price
            FROM product product1
            ORDER BY price DESC
            LIMIT ?) subquery1
          ON (purchase0.product_id = subquery1.id)
        """
}

class sortTakeFromSubqueryTest extends SQLStringTest[AllCommerceDBs, Double] {
  def testDescription = "Subquery: sortTakeFrom"
  def query() =
        testDB.tables.products.sort(_.price, Ord.DESC).take(1).flatMap(prod =>
          testDB.tables.purchases.filter(purch => prod.id == purch.productId).map(purch =>
            purch.total
          )
        )
  def sqlString = """
        SELECT purchase1.total AS res
        FROM (SELECT product0.id AS id, product0.price AS price
          FROM product product0
          ORDER BY price DESC
          LIMIT ?) subquery0
        JOIN purchase purchase1 ON (subquery0.id = purchase1.product_id)
      """
}

class sortTakeFromSubquery2Test extends SQLStringTest[AllCommerceDBs, Double] {
  def testDescription = "Subquery: sortTakeFrom (for comprehension)"
  def query() =
    for
      prod <- testDB.tables.products.sort(_.price, Ord.DESC).take(1)
      purch <- testDB.tables.purchases
      if prod.id == purch.productId
    yield purch.total

  def sqlString =
    """
          SELECT purchase1.total AS res
          FROM (SELECT product0.id AS id, product0.price AS price
            FROM product product0
            ORDER BY price DESC
            LIMIT ?) subquery0
          JOIN purchase purchase1 ON (subquery0.id = purchase1.product_id)
        """
}
class sortTakeFromAndJoinSubqueryTest extends SQLStringTest[AllCommerceDBs, (name: String, count: Int)] {
  def testDescription = "Subquery: sortTakeFromAndJoin"
  def query() =
    for
      t1 <- testDB.tables.products.sort(_.price, Ord.DESC).take(3)
      t2 <- testDB.tables.purchases.sort(_.count, Ord.DESC).take(3)
      if t1.id == t2.productId
    yield
      (name = t1.name, count = t2.count).toRow
  def sqlString = """
        SELECT
          subquery0.name AS res_0,
          subquery1.count AS res_1
        FROM (SELECT
            product0.id AS id,
            product0.name AS name,
            product0.price AS price
          FROM product product0
          ORDER BY price DESC
          LIMIT ?) subquery0
        JOIN (SELECT
            purchase1.product_id AS product_id,
            purchase1.count AS count
          FROM purchase purchase1
          ORDER BY count DESC
          LIMIT ?) subquery1
        ON (subquery0.id = subquery1.product_id)
      """
}
class sortLimitSortLimitSubqueryTest extends SQLStringTest[AllCommerceDBs, String] {
  def testDescription = "Subquery: sortLimitSortLimit"
  def query() =
    testDB.tables.products.sort(_.price, Ord.DESC).take(4).sort(_.price, Ord.DESC).take(2).map(_.name)
  def sqlString = """
        SELECT subquery0.name AS res
        FROM (SELECT
            product0.name AS name,
            product0.price AS price
          FROM product product0
          ORDER BY price DESC
          LIMIT ?) subquery0
        ORDER BY subquery0.price ASC
        LIMIT ?
      """
}

/*class sortGroupBySubqueryTest extends SQLStringTest[AllCommerceDBs, (name: String, shippingDate: LocalDate)] {
  def testDescription = "Subquery: sortGroupBy"
  def query() =
        Purchase.select.sort(_.count).take(5).groupBy(_.productId)(_.sumBy(_.total))
  def sqlString = """
        SELECT subquery0.product_id AS res_0, SUM(subquery0.total) AS res_1
        FROM (SELECT
            purchase0.product_id AS product_id,
            purchase0.count AS count,
            purchase0.total AS total
          FROM purchase purchase0
          ORDER BY count
          LIMIT ?) subquery0
        GROUP BY subquery0.product_id
      """
}
class groupByJoinSubqueryTest extends SQLStringTest[AllCommerceDBs, (name: String, shippingDate: LocalDate)] {
  def testDescription = "Subquery: groupByJoin"
  def query() =
        Purchase.select.groupBy(_.productId)(_.sumBy(_.total)).join(Product)(_._1 `=` _.id).map {
          case (productId, total, product) => (product.name, total)
        }
  def sqlString = """
        SELECT
          product1.name AS res_0,
          subquery0.res_1 AS res_1
        FROM (SELECT
            purchase0.product_id AS res_0,
            SUM(purchase0.total) AS res_1
          FROM purchase purchase0
          GROUP BY purchase0.product_id) subquery0
        JOIN product product1 ON (subquery0.res_0 = product1.id)
      """
}
*/
//class subqueryInFilterSubqueryTest extends SQLStringTest[AllCommerceDBs, Buyer] {
//  def testDescription = "Subquery: subqueryInFilter"
//  def query() =
//    testDB.tables.buyers.filter(c =>
//      testDB.tables.shipInfos.filter(s =>
//        c.id == s.buyerId
//      ).size == 0
//    )
//  def sqlString = """
//        SELECT
//          buyer0.id AS id,
//          buyer0.name AS name,
//          buyer0.date_of_birth AS date_of_birth
//        FROM buyer buyer0
//        WHERE ((SELECT
//            COUNT(1) AS res
//            FROM shipping_info shipping_info1
//            WHERE (buyer0.id = shipping_info1.buyer_id)) = ?)
//      """
//}
//class SubqueryInMapSubqueryTest extends SQLStringTest[AllCommerceDBs, (buyer: Buyer, count: Int)] {
//  def testDescription = "Subquery: subqueryInMap"
//  def query() =
//    testDB.tables.buyers.map(c =>
//      (buyer = c, count = testDB.tables.shipInfos.filter(p => c.id == p.buyerId).size)
//    )
//  def sqlString = """
//        SELECT
//          buyer0.id AS res_0_id,
//          buyer0.name AS res_0_name,
//          buyer0.date_of_birth AS res_0_date_of_birth,
//          (SELECT COUNT(1) AS res
//            FROM shipping_info shipping_info1
//            WHERE (buyer0.id = shipping_info1.buyer_id)) AS res_1
//        FROM buyer buyer0
//      """
//}
//class subqueryInMapNestedSubqueryTest extends SQLStringTest[AllCommerceDBs, (buyer: Buyer, occurances: Int)] {
//  def testDescription = "Subquery: subqueryInMapNested"
//  def query() =
//    testDB.tables.buyers.map(c =>
//      (buyer = c, occurances = testDB.tables.shipInfos.filter(p => p.buyerId == c.id).size == 1)
//    )
//  def sqlString = """
//        SELECT
//          buyer0.id AS res_0_id,
//          buyer0.name AS res_0_name,
//          buyer0.date_of_birth AS res_0_date_of_birth,
//          ((SELECT
//            COUNT(1) AS res
//            FROM shipping_info shipping_info1
//            WHERE (buyer0.id = shipping_info1.buyer_id)) = ?) AS res_1
//        FROM buyer buyer0
//      """
//}
//class subqueryInMapNestedConcatSubqueryTest extends SQLStringTest[AllCommerceDBs, (id: Int, name: String, dateOfBirth: LocalDate, occurances: Int)] {
//  def testDescription = "Subquery: subqueryInMapNested"
//  def query() =
//    testDB.tables.buyers.map(c =>
//      c.concat((occurances = testDB.tables.shipInfos.filter(p => p.buyerId == c.id).size == 1))
//    )
//  def sqlString = """
//        SELECT
//          buyer0.id AS res_0_id,
//          buyer0.name AS res_0_name,
//          buyer0.date_of_birth AS res_0_date_of_birth,
//          ((SELECT
//            COUNT(1) AS res
//            FROM shipping_info shipping_info1
//            WHERE (buyer0.id = shipping_info1.buyer_id)) = ?) AS res_1
//        FROM buyer buyer0
//      """
//}

class selectLimitUnionSelectSubqueryTest extends SQLStringTest[AllCommerceDBs, String] {
  def testDescription = "Subquery: selectLimitUnionSelect"
  def query() =
    testDB.tables.buyers.map(_.name.toLowerCase).take(2).unionAll(testDB.tables.products.map(_.name.toLowerCase))
  def sqlString = """
        SELECT subquery0.res AS res
        FROM (SELECT
            LOWER(buyer0.name) AS res
          FROM buyer buyer0
          LIMIT ?) subquery0
        UNION ALL
        SELECT LOWER(product0.kebab_case_name) AS res
        FROM product product0
      """
}

class selectUnionSelectLimitSubqueryTest extends SQLStringTest[AllCommerceDBs, String] {
  def testDescription = "Subquery: selectUnionSelectLimit"
  def query() =
    testDB.tables.buyers.map(_.name.toLowerCase).unionAll(testDB.tables.products.map(_.name.toLowerCase).take(2))
  def sqlString = """
        SELECT LOWER(buyer0.name) AS res
        FROM buyer buyer0
        UNION ALL
        SELECT subquery0.res AS res
        FROM (SELECT
            LOWER(product0.kebab_case_name) AS res
          FROM product product0
          LIMIT ?) subquery0
      """
}
class ExceptAggregateSubqueryTest extends SQLStringTest[AllCommerceDBs, (max: Double, min: Double)] {
  def testDescription = "Subquery: exceptAggregate"
  def query() =
    testDB.tables.products
      .map(p => (name = p.name.toLowerCase, price = p.price).toRow)
      .except(
        testDB.tables.products.map(p => (name = p.name.toLowerCase, price = p.price).toRow)
      )
      .map(ps => (max = ps.price.max, min = ps.price.min).toRow)
  def sqlString = """
        SELECT
          MAX(subquery0.res_1) AS res_0,
          MIN(subquery0.res_1) AS res_1
        FROM (SELECT
            LOWER(product0.name) AS res_0,
            product0.price AS res_1
          FROM product product0
          EXCEPT
          SELECT
            LOWER(product0.kebab_case_name) AS res_0,
            product0.price AS res_1
          FROM product product0) subquery0
      """
}

class UnionAllAggregateSubqueryTest extends SQLStringTest[AllCommerceDBs, (max: Double, min: Double)] {
  def testDescription = "Subquery: unionAllAggregate"
  def query() =
    testDB.tables.products
      .map(p => (name = p.name.toLowerCase, price = p.price).toRow)
      .unionAll(testDB.tables.products
        .map(p2 => (name = p2.name.toLowerCase, price = p2.price).toRow)
      )
      .map(p => (max = p.price.max, min = p.price.min).toRow)
  def sqlString = """
        SELECT
          MAX(subquery0.res_1) AS res_0,
          MIN(subquery0.res_1) AS res_1
        FROM (SELECT product0.price AS res_1
          FROM product product0
          UNION ALL
          SELECT product0.price AS res_1
          FROM product product0) subquery0
      """
}
//class DeeplyNestedSubqueryTest extends SQLStringTest[AllCommerceDBs, (max: Double, min: Double)] {
//  def testDescription = "Subquery: deeplyNested"
//  def query() =
//        Buyer.select.map { buyer =>
//          buyer.name ->
//            ShippingInfo.select
//              .filter(_.buyerId === buyer.id)
//              .map { shippingInfo =>
//                Purchase.select
//                  .filter(_.shippingInfoId === shippingInfo.id)
//                  .map { purchase =>
//                    Product.select
//                      .filter(_.id === purchase.productId)
//                      .map(_.price)
//                      .sorted
//                      .desc
//                      .take(1)
//                      .toExpr
//                  }
//                  .sorted
//                  .desc
//                  .take(1)
//                  .toExpr
//              }
//              .sorted
//              .desc
//              .take(1)
//              .toExpr
//        }
//  def sqlString = """
//      SELECT
//        buyer0.name AS res_0,
//        (SELECT
//          (SELECT
//            (SELECT product3.price AS res
//            FROM product product3
//            WHERE (product3.id = purchase2.product_id)
//            ORDER BY res DESC
//            LIMIT ?) AS res
//          FROM purchase purchase2
//          WHERE (purchase2.shipping_info_id = shipping_info1.id)
//          ORDER BY res DESC
//          LIMIT ?) AS res
//        FROM shipping_info shipping_info1
//        WHERE (shipping_info1.buyer_id = buyer0.id)
//        ORDER BY res DESC
//        LIMIT ?) AS res_1
//      FROM buyer buyer0
//      """
//}

