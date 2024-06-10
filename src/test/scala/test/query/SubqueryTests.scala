package test.query.subquery

import test.SQLStringQueryTest
import test.query.{AllCommerceDBs, Buyer, commerceDBs}
import tyql.*
import tyql.Expr.{max, min, toRow}

import language.experimental.namedTuples
import NamedTuple.*
// import scala.language.implicitConversions

import java.time.LocalDate

class sortTakeJoinSubqueryTest extends SQLStringQueryTest[AllCommerceDBs, Double] {
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

class sortTake2JoinSubqueryTest extends SQLStringQueryTest[AllCommerceDBs, Double] {
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

class sortTakeFromSubqueryTest extends SQLStringQueryTest[AllCommerceDBs, Double] {
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

class sortTakeFromSubquery2Test extends SQLStringQueryTest[AllCommerceDBs, Double] {
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
class sortTakeFromAndJoinSubqueryTest extends SQLStringQueryTest[AllCommerceDBs, (name: String, count: Int)] {
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
class sortLimitSortLimitSubqueryTest extends SQLStringQueryTest[AllCommerceDBs, String] {
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
class subqueryInFilterSubqueryTest extends SQLStringQueryTest[AllCommerceDBs, Buyer] {
  def testDescription = "Subquery: subqueryInFilter"
  def query() =
    testDB.tables.buyers.filter(c =>
      testDB.tables.shipInfos.filter(s =>
        c.id == s.buyerId
      ).size == 0
    )
  def sqlString = """
        SELECT
          buyer0.id AS id,
          buyer0.name AS name,
          buyer0.date_of_birth AS date_of_birth
        FROM buyer buyer0
        WHERE ((SELECT
            COUNT(1) AS res
            FROM shipping_info shipping_info1
            WHERE (buyer0.id = shipping_info1.buyer_id)) = ?)
      """
}
class SubqueryInMapSubqueryTest extends SQLStringQueryTest[AllCommerceDBs, (buyer: Buyer, count: Int)] {
  def testDescription = "Subquery: subqueryInMap"
  def query() =
    testDB.tables.buyers.map(c =>
      (buyer = c, count = testDB.tables.shipInfos.filter(p => c.id == p.buyerId).size).toRow
    )
  def sqlString = """
        SELECT
          buyer0.id AS res_0_id,
          buyer0.name AS res_0_name,
          buyer0.date_of_birth AS res_0_date_of_birth,
          (SELECT COUNT(1) AS res
            FROM shipping_info shipping_info1
            WHERE (buyer0.id = shipping_info1.buyer_id)) AS res_1
        FROM buyer buyer0
      """
}
class subqueryInMapNestedSubqueryTest extends SQLStringQueryTest[AllCommerceDBs, (buyer: Buyer, occurances: Boolean)] {
  def testDescription = "Subquery: subqueryInMapNested"
  def query() =
    testDB.tables.buyers.map(c =>
      (buyer = c, occurances = testDB.tables.shipInfos.filter(p => p.buyerId == c.id).size == 1).toRow
    )
  def sqlString = """
        SELECT
          buyer0.id AS res_0_id,
          buyer0.name AS res_0_name,
          buyer0.date_of_birth AS res_0_date_of_birth,
          ((SELECT
            COUNT(1) AS res
            FROM shipping_info shipping_info1
            WHERE (buyer0.id = shipping_info1.buyer_id)) = ?) AS res_1
        FROM buyer buyer0
      """
}
class subqueryInMapNestedConcatSubqueryTest extends SQLStringQueryTest[AllCommerceDBs, (id: Int, name: String, dateOfBirth: LocalDate, occurances: Int)] {
  def testDescription = "Subquery: subqueryInMapNested"
  def query() =
    testDB.tables.buyers.map(c =>
      (id = c.id, name = c.name, dateOfBirth = c.dateOfBirth).toRow.concat((occurances = testDB.tables.shipInfos.filter(p => p.buyerId == c.id).size))
    )
  def sqlString = """
        SELECT
          buyer0.id AS res_0_id,
          buyer0.name AS res_0_name,
          buyer0.date_of_birth AS res_0_date_of_birth,
          ((SELECT
            COUNT(1) AS res
            FROM shipping_info shipping_info1
            WHERE (buyer0.id = shipping_info1.buyer_id)) = ?) AS res_1
        FROM buyer buyer0
      """
}

class selectLimitUnionSelectSubqueryTest extends SQLStringQueryTest[AllCommerceDBs, String] {
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

class selectUnionSelectLimitSubqueryTest extends SQLStringQueryTest[AllCommerceDBs, String] {
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
class ExceptAggregateSubqueryTest extends SQLStringQueryTest[AllCommerceDBs, (max: Double, min: Double)] {
  def testDescription = "Subquery: exceptAggregate"
  def query() =
    import Aggregation.toRow as AggrToRow
    testDB.tables.products
      .map(p => (name = p.name.toLowerCase, price = p.price).toRow)
      .except(
        testDB.tables.products.map(p => (name = p.name.toLowerCase, price = p.price).toRow)
      )
      .map(ps => (max = max(ps.price), min = min(ps.price)).AggrToRow)
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

class UnionAllAggregateSubqueryTest extends SQLStringQueryTest[AllCommerceDBs, (max: Double, min: Double)] {
  def testDescription = "Subquery: unionAllAggregate"
  def query() =
    import Aggregation.toRow as AggrToRow
    testDB.tables.products
      .map(p => (name = p.name.toLowerCase, price = p.price).toRow)
      .unionAll(testDB.tables.products
        .map(p2 => (name = p2.name.toLowerCase, price = p2.price).toRow)
      )
      .map(p => (max = max(p.price), min = min(p.price)).AggrToRow)
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
