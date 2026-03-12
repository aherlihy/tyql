package test.query.scope

import test.SQLStringQueryTest
import test.query.{AllCommerceDBs, Buyer, commerceDBs, Product, Purchase}
import tyql.*
import tyql.Expr.{max, min, toRow}

import language.experimental.namedTuples
import NamedTuple.*
// import scala.language.implicitConversions

class ScopeTest extends SQLStringQueryTest[AllCommerceDBs, Int] {
  def testDescription = "No subquery, filter/map one relation"

  def query() =
    testDB.tables.purchases.flatMap(purch =>
      testDB.tables.products
        .filter(prod => prod.id > 1)
        .map(prod => prod.id)
    )

  def expectedQueryPattern =
    """
            SELECT product$B.id
            FROM
              purchase as purchase$A,
              product as product$B
            WHERE product$B.id > 1
          """
}

class Scope1Test extends SQLStringQueryTest[AllCommerceDBs, Int] {
  def testDescription = "No subquery, filter on both relations"

  def query() =
    testDB.tables.purchases.flatMap(purch =>
      testDB.tables.products.
        filter(prod => prod.id == purch.productId).
        map(prod => prod.id)
    )

  def expectedQueryPattern =
    """
          SELECT product$B.id
          FROM
            purchase as purchase$A, product as product$B
          WHERE product$B.id = purchase$A.productId
        """
}

class Scope2Test extends SQLStringQueryTest[AllCommerceDBs, (purchId: Int, prodPrice: Double)] {
  def testDescription = "No subquery, filter and map on both relations"

  def query() =
    testDB.tables.purchases.flatMap(purch =>
      testDB.tables.products.
        filter(prod => prod.id == purch.productId).
        map(prod => (purchId = purch.id, prodPrice = prod.price).toRow)
    )

  def expectedQueryPattern =
    """
            SELECT purchase$A.id as purchId, product$B.price as prodPrice
            FROM
              purchase as purchase$A, product as product$B
            WHERE product$B.id = purchase$A.productId
          """
}

class ScopeSubqueryTest extends SQLStringQueryTest[AllCommerceDBs, Int] {
  def testDescription = "Subquery (take), filter/map one relation"

  def query() =
    testDB.tables.purchases.take(1).flatMap(purch =>
      testDB.tables.products.take(2)
        .filter(prod => prod.id > 1)
        .map(prod => prod.id)
    )

  def expectedQueryPattern =
    """
            SELECT subquery$D.id
            FROM
              (SELECT * FROM purchase as purchase$A LIMIT 1) as subquery$C,
              (SELECT * FROM product as product$B LIMIT 2) as subquery$D
            WHERE subquery$D.id > 1
          """
}

class ScopeSubquery1Test extends SQLStringQueryTest[AllCommerceDBs, Int] {
  def testDescription = "Subquery (take), filter on both relations"

  def query() =
    testDB.tables.purchases.take(1).flatMap(purch =>
      testDB.tables.products.take(2).
        filter(prod => prod.id == purch.productId).
        map(prod => prod.id)
    )

  def expectedQueryPattern =
    """
          SELECT subquery$D.id
          FROM
              (SELECT * FROM purchase as purchase$A LIMIT 1) as subquery$C,
              (SELECT * FROM product as product$B LIMIT 2) as subquery$D
          WHERE subquery$D.id = subquery$C.productId
        """
}

class ScopeSubquery2Test extends SQLStringQueryTest[AllCommerceDBs, (purchId: Int, prodPrice: Double)] {
  def testDescription = "Subquery (take) filter and map on both relations"

  def query() =
    testDB.tables.purchases.take(1).flatMap(purch =>
      testDB.tables.products.take(2).
        filter(prod => prod.id == purch.productId).
        map(prod => (purchId = purch.id, prodPrice = prod.price).toRow)
    )

  def expectedQueryPattern =
    """
        SELECT subquery$C.id as purchId, subquery$D.price as prodPrice
        FROM
          (SELECT * FROM purchase as purchase$A LIMIT 1) as subquery$C,
          (SELECT * FROM product as product$B LIMIT 2) as subquery$D
        WHERE subquery$D.id = subquery$C.productId
          """
}

class ScopeSubquery3Test extends SQLStringQueryTest[AllCommerceDBs, (purchId: Int, prodPrice: Double)] {
  def testDescription = "Subquery (take) filter and map on both relations"

  def query() =
    testDB.tables.purchases.map(m => (purchasesId = m.id, purchasesProductId = m.productId)).take(1).flatMap(purch =>
      testDB.tables.products.map(m => (productsId = m.id, productsPrice = m.price)).take(2).
        filter(prod => prod.productsId == purch.purchasesProductId).
        map(prod => (purchId = purch.purchasesId, prodPrice = prod.productsPrice).toRow)
    )

  def expectedQueryPattern =
    """
          SELECT subquery$C.purchasesId as purchId, subquery$D.productsPrice as prodPrice
          FROM
            (SELECT purchase$A.id as purchasesId, purchase$A.productId as purchasesProductId FROM purchase as purchase$A LIMIT 1) as subquery$C,
            (SELECT product$B.id as productsId, product$B.price as productsPrice FROM product as product$B LIMIT 2) as subquery$D
          WHERE subquery$D.productsId = subquery$C.purchasesProductId
            """
}

class ScopeSubquery4Test extends SQLStringQueryTest[AllCommerceDBs, (purchId: Int, prodPrice: Double)] {
  def testDescription = "Subquery (sort) on both"
  def query() =
    testDB.tables.purchases.sort(_.total, Ord.ASC).flatMap(purch =>
      testDB.tables.products.sort(_.price, Ord.DESC).
        filter(prod => prod.id == purch.productId).
        map(prod => (purchId = purch.id, prodPrice = prod.price).toRow)
    )
  def expectedQueryPattern = """
        SELECT subquery$C.id as purchId, subquery$D.price as prodPrice
        FROM
          (SELECT *
           FROM
           purchase as purchase$B
           ORDER BY total ASC) as subquery$C,
          (SELECT *
           FROM product as product$A
           ORDER BY price DESC) as subquery$D
        WHERE subquery$D.id = subquery$C.productId
      """
}

class ScopeSubquery5Test extends SQLStringQueryTest[AllCommerceDBs, (t: Int, p: Int, b: Int)] {
  def testDescription = "Subquery: Cross references"
  def query() =
    testDB.tables.purchases.flatMap(pur =>
      testDB.tables.buyers.flatMap(buy =>
        testDB.tables.products
          .filter(prod => prod.id == pur.productId && prod.id == buy.id)
          .map(prod => (t = pur.id, p = prod.id, b = buy.id).toRow)
      )
    )
  def expectedQueryPattern = """
	    SELECT purchase$8.id as t, product$10.id as p, buyers$9.id as b
	    FROM purchase as purchase$8, buyers as buyers$9, product as product$10
	    WHERE product$10.id = purchase$8.productId AND product$10.id = buyers$9.id
      """
}

class ScopeSubquery6Test extends SQLStringQueryTest[AllCommerceDBs, (t: Int, p: Int, b: Int)] {
  def testDescription = "Subquery: Cross references with breaker"
  def query() =
    testDB.tables.purchases.flatMap(pur =>
      testDB.tables.buyers.flatMap(buy =>
        testDB.tables.products
          .filter(prod => prod.id == pur.productId && prod.id == buy.id)
          .map(prod => (t = pur.id, p = prod.id, b = buy.id).toRow)
      ).limit(10)
    )
  def expectedQueryPattern = """
      SELECT subquery$43
      FROM
        purchase as purchase$38,
        (SELECT purchase$38.id as t, product$40.id as p, buyers$39.id as b FROM buyers as buyers$39, product as product$40 WHERE product$40.id = purchase$38.productId AND product$40.id = buyers$39.id LIMIT 10) as subquery$43
      """
}

