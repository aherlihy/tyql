package test.query.subquery

import test.SQLStringQueryTest
import test.query.{AllCommerceDBs, Buyer, commerceDBs, Product, Purchase}
import tyql.*
import tyql.Expr.{max, min, toRow}

import language.experimental.namedTuples
import NamedTuple.*
// import scala.language.implicitConversions

import java.time.LocalDate

class SortTakeJoinSubqueryTest extends SQLStringQueryTest[AllCommerceDBs, Double] {
  def testDescription = "Subquery: sortTakeJoin"
  def query() =
    testDB.tables.purchases.flatMap(purch =>
      testDB.tables.products.sort(_.price, Ord.DESC).take(1)
        .filter(prod => prod.id == purch.id)
        .map(prod => purch.total)
    )
  def expectedQueryPattern = """
        SELECT purchase$A.total
        FROM purchase as purchase$A,
          (SELECT *
          FROM product as product$B
          ORDER BY price DESC
          LIMIT 1) as subquery$C
        WHERE subquery$C.id = purchase$A.id
      """
}

class SortTake2JoinSubqueryTest extends SQLStringQueryTest[AllCommerceDBs, Double] {
  def testDescription = "Subquery: sortTakeJoin (for comprehension)"
  def query() =

    for
      purch <- testDB.tables.purchases
      prod <- testDB.tables.products.sort(_.price, Ord.DESC).take(1)
      if prod.id == purch.id
    yield purch.total

  def expectedQueryPattern =
    """
    SELECT purchase$A.total
    FROM purchase as purchase$A,
  (SELECT *
    FROM product as product$B
    ORDER BY price DESC
    LIMIT 1) as subquery$C
    WHERE subquery$C.id = purchase$A.id
        """
}

class SortTakeFromSubqueryTest extends SQLStringQueryTest[AllCommerceDBs, Double] {
  def testDescription = "Subquery: sortTakeFrom"
  def query() =
    testDB.tables.products.sort(_.price, Ord.DESC).take(1).flatMap(prod =>
      testDB.tables.purchases.filter(purch => prod.id == purch.productId).map(purch =>
        purch.total
      )
    )
  def expectedQueryPattern = """
        SELECT purchase$A.total
        FROM
        (SELECT *
          FROM product as product$B
          ORDER BY price DESC
          LIMIT 1) as subquery$C,
        purchase as purchase$D
        WHERE subquery$C.id = purchase$D.productId
      """
}

class SortTakeFromSubquery2Test extends SQLStringQueryTest[AllCommerceDBs, Double] {
  def testDescription = "Subquery: sortTakeFrom (reversed)"
  def query() =
    testDB.tables.purchases.flatMap(purch =>
      testDB.tables.products.sort(_.price, Ord.DESC).take(1).filter(prod => prod.id == purch.productId).map(prod =>
        purch.total
      )
    )
  def expectedQueryPattern = """
        SELECT purchase$A.total
        FROM
        purchase as purchase$D,
        (SELECT *
          FROM product as product$B
          ORDER BY price DESC
          LIMIT 1) as subquery$C
        WHERE subquery$C.id = purchase$D.productId
      """
}

class SortTakeFromSubquery3Test extends SQLStringQueryTest[AllCommerceDBs, Double] {
  def testDescription = "Subquery: sortTakeFrom (both)"
  def query() =
    testDB.tables.purchases.sort(_.total, Ord.ASC).take(2).flatMap(purch =>
      testDB.tables.products.sort(_.price, Ord.DESC).take(1).
        filter(prod => prod.id == purch.productId).
        map(prod =>
          purch.total
        )
    )
  def expectedQueryPattern = """
        SELECT subquery$E.total
        FROM
         (SELECT *
          FROM purchase as purchase$A
          ORDER BY total ASC
          LIMIT 2) as subquery$E,
         (SELECT *
          FROM product as product$B
          ORDER BY price DESC
          LIMIT 1) as subquery$D
        WHERE subquery$D.id = subquery$E.productId
      """
}

class NestedSubqueryTest extends SQLStringQueryTest[AllCommerceDBs, Int] {
  def testDescription = "Subquery: nest using sort"
  def query() =
    testDB.tables.purchases.sort(_.total, Ord.ASC).flatMap(purch =>
      testDB.tables.products.sort(_.price, Ord.DESC).
        filter(prod => prod.id == purch.productId).
        map(prod => prod.id)
    )
  def expectedQueryPattern = """
        SELECT subquery$B.id
        FROM
          (SELECT *
           FROM
           purchase as purchase$D
           ORDER BY total ASC) as subquery$C,
          (SELECT *
           FROM product as product$A
           ORDER BY price DESC) as subquery$B
        WHERE subquery$B.id = subquery$C.productId
      """
}


class SimpleNestedSubquery1Test extends SQLStringQueryTest[AllCommerceDBs, Int] {
  def testDescription = "Subquery: nested map"
  def query() =
    testDB.tables.purchases.flatMap(purch =>
      testDB.tables.products.
        filter(prod => prod.id == purch.productId).
        map(prod => prod.id)
    )
  def expectedQueryPattern = """
        SELECT product$A.id
        FROM
          purchase as purchase$B,
          product as product$A
        WHERE product$A.id = purchase$B.productId
      """
}

class SimpleNestedSubquery2Test extends SQLStringQueryTest[AllCommerceDBs, Int] {
  def testDescription = "Subquery: nested map + inner op"
  def query() =
    testDB.tables.purchases.flatMap(purch =>
      testDB.tables.products.
        drop(1).
        filter(prod => prod.id == purch.productId).
        map(prod => prod.id)
    )
  def expectedQueryPattern = """
        SELECT subquery$C.id
        FROM
          purchase as purchase$B,
          (SELECT * FROM product as product$A OFFSET 1) as subquery$C
        WHERE subquery$C.id = purchase$B.productId
      """
}

class SimpleNestedFilterSubqueryTest extends SQLStringQueryTest[AllCommerceDBs, Purchase] {
  def testDescription = "Subquery: nested filters"
  def query() =
    testDB.tables.purchases
//      .drop(1)
      .filter(purch => purch.id == 1)
//      .take(2)
      .filter(purch2 => purch2.id == 2)
//      .map(_.id)
  def expectedQueryPattern = """
    SELECT * FROM purchase as purchase$A WHERE purchase$A.id = 2 AND purchase$A.id = 1
      """
}

class SimpleNestedFilterMapSubqueryTest extends SQLStringQueryTest[AllCommerceDBs, Int] {
  def testDescription = "Subquery: nested filters with map"

  def query() =
    testDB.tables.purchases
      //      .drop(1)
      .filter(purch => purch.id == 1)
      //      .take(2)
      .filter(purch2 => purch2.id == 2)

      .map(_.id)
  def expectedQueryPattern =
    """
      SELECT purchase$A.id FROM purchase as purchase$A WHERE purchase$A.id = 2 AND purchase$A.id = 1
        """
}

class SimpleNestedFilterSubquery2Test extends SQLStringQueryTest[AllCommerceDBs, Purchase] {
  def testDescription = "Subquery: nested filters, outer operation"
  def query() =
    testDB.tables.purchases
      .drop(1)
      .filter(purch => purch.id == 1)
      //      .take(2)
      .filter(purch2 => purch2.id == 2)
//      .map(_.id)
  def expectedQueryPattern = """
        SELECT *
        FROM
          (SELECT * FROM purchase as purchase$A OFFSET 1) as subquery$B
        WHERE subquery$B.id = 2 AND subquery$B.id = 1
      """
}

class SimpleNestedFilterMapSubquery2Test extends SQLStringQueryTest[AllCommerceDBs, Int] {
  def testDescription = "Subquery: nested filters, outer operation, with map"

  def query() =
    testDB.tables.purchases
      .drop(1)
      .filter(purch => purch.id == 1)
      //      .take(2)
      .filter(purch2 => purch2.id == 2)
      .map(_.id)
  def expectedQueryPattern =
    """
          SELECT subquery$B.id
          FROM
            (SELECT * FROM purchase as purchase$A OFFSET 1) as subquery$B
          WHERE subquery$B.id = 2 AND subquery$B.id = 1
        """
}

class SimpleNestedFilterSubquery3Test extends SQLStringQueryTest[AllCommerceDBs, Purchase] {
  def testDescription = "Subquery: nested filters, inner operation"
  def query() =
    testDB.tables.purchases
//      .drop(1)
      .filter(purch => purch.id == 1)
      .take(2)
      .filter(purch2 => purch2.id == 2)
  //      .map(_.id)
  def expectedQueryPattern = """
        SELECT *
        FROM
          (SELECT * FROM purchase as purchase$A WHERE purchase$A.id = 1 LIMIT 2) as subquery$B
        WHERE subquery$B.id = 2
      """
}

class SimpleNestedFilterMapSubquery3Test extends SQLStringQueryTest[AllCommerceDBs, Int] {
  def testDescription = "Subquery: nested filters, inner operation wth map"

  def query() =
    testDB.tables.purchases
      //      .drop(1)
      .filter(purch => purch.id == 1)
      .take(2)
      .filter(purch2 => purch2.id == 2)
      .map(_.id)
  def expectedQueryPattern =
    """
          SELECT subquery$B.id
          FROM
            (SELECT * FROM purchase as purchase$A WHERE purchase$A.id = 1 LIMIT 2) as subquery$B
          WHERE subquery$B.id = 2
        """
}

class SimpleNestedFilterSubquery4Test extends SQLStringQueryTest[AllCommerceDBs, Purchase] {
  def testDescription = "Subquery: nested filters, inner+outer operation"
  def query() =
    testDB.tables.purchases
      .drop(1)
      .filter(purch => purch.id == 1)
      .take(2)
      .filter(purch2 => purch2.id == 2)
  //      .map(_.id)
  def expectedQueryPattern = """
        SELECT *
        FROM
          (SELECT * FROM
             (SELECT * FROM purchase as purchase$A OFFSET 1) as subquery$B
           WHERE subquery$B.id = 1 LIMIT 2) as subquery$C
        WHERE subquery$C.id = 2
      """
}

class SimpleNestedFilterMapSubquery4Test extends SQLStringQueryTest[AllCommerceDBs, Int] {
  def testDescription = "Subquery: nested filters, inner+outer operation with map"
  def query() =
    testDB.tables.purchases
      .drop(1)
      .filter(purch => purch.id == 1)
      .take(2)
      .filter(purch2 => purch2.id == 2)
      .map(_.id)
  def expectedQueryPattern = """
        SELECT subquery$C.id
        FROM
          (SELECT * FROM
             (SELECT * FROM purchase as purchase$A OFFSET 1) as subquery$B
           WHERE subquery$B.id = 1 LIMIT 2) as subquery$C
        WHERE subquery$C.id = 2
      """
}

class SimpleNestedSubqueryTest extends SQLStringQueryTest[AllCommerceDBs, Int] {
  def testDescription = "Subquery: nest map + outer op"
  def query() =
    testDB.tables.purchases.drop(1).flatMap(purch =>
      testDB.tables.products.
        filter(prod => prod.id == purch.productId).
        map(prod => prod.id)
    )
  def expectedQueryPattern = """
        SELECT product$A.id
        FROM
          (SELECT * FROM purchase as purchase$C OFFSET 1) as subquery$B,
          product as product$A
        WHERE product$A.id = subquery$B.productId
      """
}
class SortTakeFromSubquery4Test extends SQLStringQueryTest[AllCommerceDBs, Double] {
  def testDescription = "Subquery: sortTakeFrom, outer op"
  def query() =
    for
      prod <- testDB.tables.products.sort(_.price, Ord.DESC).take(1)
      purch <- testDB.tables.purchases
      if prod.id == purch.productId
    yield purch.total

  def expectedQueryPattern =
    """
        SELECT purchase$A.total
        FROM
        (SELECT *
          FROM product as product$B
          ORDER BY price DESC
          LIMIT 1) as subquery$C,
        purchase as purchase$D
        WHERE subquery$C.id = purchase$D.productId
        """
}
class SortTakeFromAndJoinSubqueryTest extends SQLStringQueryTest[AllCommerceDBs, (name: String, count: Int)] {
  def testDescription = "Subquery: sortTakeFrom (for comprehension)"
  def query() =
    for
      t1 <- testDB.tables.products.sort(_.price, Ord.DESC).take(3)
      t2 <- testDB.tables.purchases.sort(_.count, Ord.DESC).take(4)
      if t1.id == t2.productId
    yield
      (name = t1.name, count = t2.count).toRow
  def expectedQueryPattern = """
      SELECT subquery$A.name as name, subquery$B.count as count
      FROM
        (SELECT * FROM product as product$Q ORDER BY price DESC LIMIT 3) as subquery$A,
        (SELECT * FROM purchase as purchase$R ORDER BY count DESC LIMIT 4) as subquery$B
      WHERE subquery$A.id = subquery$B.productId
      """
}
class SingleJoinForNestedJoin extends SQLStringQueryTest[AllCommerceDBs, (name: String, id: Int)] {
  def testDescription = "Subquery: sortTakeJoin, double nested"

  def query() =
    testDB.tables.purchases.flatMap(purch =>
      testDB.tables.products.map(prod =>
        (name = prod.name, id = purch.id).toRow
      )
    )

  def expectedQueryPattern =
    """
      SELECT product$A.name as name, purchase$B.id as id FROM purchase as purchase$B, product as product$A
    """
}

class NestedJoinSubqueryTest extends SQLStringQueryTest[AllCommerceDBs, String] {
  def testDescription = "Subquery: nested cartesian product without project"

  def query() =
    testDB.tables.purchases.flatMap(purch =>
      testDB.tables.products.map(s => s)
    ).flatMap(purch =>
      testDB.tables.purchases.flatMap(purch =>
          testDB.tables.products.map(s => s)
        )
        //        .filter(prod => prod.id == purch.id)
        .map(prod => purch.name)
    )


  def expectedQueryPattern =
    """
      SELECT subquery$A.name
      FROM
        (SELECT product$X FROM purchase as purchase$Y, product as product$X) as subquery$A,
        (SELECT product$R FROM purchase as purchase$S, product as product$R) as subquery$B
    """
}

class NestedJoinSubquery2Test extends SQLStringQueryTest[AllCommerceDBs, Int] {
  def testDescription = "Subquery: nested cartesian product with project"

  def query() =
    testDB.tables.purchases.flatMap(purch =>
      testDB.tables.products.map(s => (prodId = s.id))
    ).flatMap(purch =>
      testDB.tables.purchases.flatMap(purch =>
          testDB.tables.products.map(s => (prodId = s.id))
        )
        //        .filter(prod => prod.id == purch.id)
        .map(prod => purch.prodId)
    )


  def expectedQueryPattern =
    """
        SELECT subquery$A.prodId
        FROM
          (SELECT product$C.id as prodId FROM purchase as purchase$X, product as product$C) as subquery$A,
          (SELECT product$D.id as prodId FROM purchase as purchase$Y, product as product$D) as subquery$B
      """
}

class NestedJoinSubquery3Test extends SQLStringQueryTest[AllCommerceDBs, (name: String, id: Int, sd: LocalDate)] {
  def testDescription = "Subquery: nested flatMaps"

  def query() =
    testDB.tables.purchases.flatMap(purch =>
      testDB.tables.products.flatMap(prod =>
        testDB.tables.shipInfos.map(si =>
          (name = prod.name, id = purch.id, sd = si.shippingDate).toRow
        )
      )
    )


  def expectedQueryPattern =
    """
      SELECT product$A.name as name, purchase$B.id as id, shippingInfo$C.shippingDate as sd
      FROM
        purchase as purchase$B,
        product as product$A,
        shippingInfo as shippingInfo$C
    """
}

class NestedJoinSubquery4Test extends SQLStringQueryTest[AllCommerceDBs, (name: String, id: Int, sd: LocalDate)] {
  def testDescription = "Subquery: nested flatMaps with join condition"

  def query() =
    testDB.tables.purchases.flatMap(purch =>
      testDB.tables.products.flatMap(prod =>
        testDB.tables.shipInfos.filter(si =>
          purch.id == prod.id && prod.id == si.id
        )
        .map(si =>
          (name = prod.name, id = purch.id, sd = si.shippingDate).toRow
        )
      )
    )


  def expectedQueryPattern =
    """
      SELECT product$A.name as name, purchase$B.id as id, shippingInfo$C.shippingDate as sd
      FROM
        purchase as purchase$B,
        product as product$A,
        shippingInfo as shippingInfo$C
      WHERE purchase$B.id = product$A AND product$A.id = shippingInfo$C.id
    """
}

class NestedJoinSubquery5Test extends SQLStringQueryTest[AllCommerceDBs, (name: String, id: Int, sd: LocalDate)] {
  def testDescription = "Subquery: nested flatMaps with join condition + intermediate filter"

  def query() =
    testDB.tables.purchases.filter(p => p.id == 2).flatMap(purch =>
      testDB.tables.products.filter(p => p.id == 1).flatMap(prod =>
        testDB.tables.shipInfos.filter(si =>
            purch.id == prod.id && prod.id == si.id
          )
          .map(si =>
            (name = prod.name, id = purch.id, sd = si.shippingDate).toRow
          )
      )
    )


  def expectedQueryPattern =
    """
      SELECT product$A.name as name, purchase$B.id as id, shippingInfo$C.shippingDate as sd
      FROM
        purchase as purchase$B,
        product as product$A,
        shippingInfo as shippingInfo$C
      WHERE (purchase$B.id = 2 AND product$A.id = 1 AND purchase$B.id = product$A AND product$A.id = shippingInfo$C.id)
    """
}

class NestedJoinSubquery6Test extends SQLStringQueryTest[AllCommerceDBs, Product] {
  def testDescription = "Subquery: flatmap with table"

  def query() =
    testDB.tables.purchases.flatMap(purch =>
      testDB.tables.products
    )


  def expectedQueryPattern =
    """
      SELECT product$B FROM purchase as purchase$A, product as product$B
      """
}

class NestedJoinSubquery7Test extends SQLStringQueryTest[AllCommerceDBs, Product] {
  def testDescription = "Subquery: flatmap with filtered table"

  def query() =
    testDB.tables.purchases.flatMap(purch =>
      testDB.tables.products.filter(s => s.name == "test")
    )


  def expectedQueryPattern =
    """
      SELECT product$B
      FROM
        purchase as purchase$A,
        product as product$B
      WHERE product$B.name = "test"
      """
}

class NestedJoinSubquery8Test extends SQLStringQueryTest[AllCommerceDBs, Product] {
  def testDescription = "Subquery: flatmap with filtered outer table"

  def query() =
    testDB.tables.purchases.filter(s => s.id == 1).flatMap(purch =>
      testDB.tables.products.filter(s => s.name == "test")
    )


  def expectedQueryPattern =
    """
      SELECT product$B
      FROM
        purchase as purchase$A,
        product as product$B
      WHERE (purchase$A.id = 1 AND product$B.name = "test")
      """
}

class NestedJoinSubquery9Test extends SQLStringQueryTest[AllCommerceDBs, (newId: Int)] {
  def testDescription = "Subquery: flatmap with map on inner table"

  def query() =
    testDB.tables.purchases
      .flatMap(purch =>
        testDB.tables.products.filter(s => s.name == "test").map(s => (newId = s.id))
      )


  def expectedQueryPattern =
    """
      SELECT product$B.id as newId
      FROM
        purchase as purchase$A
        product as product$B
      WHERE product$B.name = "test"
      """
}


class NestedJoinSubquery11Test extends SQLStringQueryTest[AllCommerceDBs, (innerK1: Int, innerK2: String)] {
  def testDescription = "Subquery: flatmap with flatmap on lhs"

  def query() =
    val lhs = testDB.tables.purchases.flatMap(p1 =>
      testDB.tables.products.map(p2 => (outerK1 = p1.id, outerK2 = p2.id))
    )

    lhs.flatMap(p3 =>
      testDB.tables.buyers.map(p4 => (innerK1 = p3.outerK1, innerK2 = p4.name))
    )


  def expectedQueryPattern =
    """
      SELECT
        subquery$A.outerK1 as innerK1, buyers$B.name as innerK2
      FROM
        (SELECT
          purchase$C.id as outerK1, product$D.id as outerK2
        FROM
          purchase as purchase$C,
          product as product$D) as subquery$A,
        buyers as buyers$B
      """
}

class NestedJoinSubquery12Test extends SQLStringQueryTest[AllCommerceDBs, (innerK1: Int, innerK2: String, innerK3: Int)] {
  def testDescription = "Subquery: flatmap with flatmap on lhs and rhs"

  def query() =
    val lhs = testDB.tables.purchases.flatMap(p1 =>
      testDB.tables.products.map(p2 => (outerK1 = p1.id, outerK2 = p2.id))
    )

    lhs.flatMap(p3 =>
      testDB.tables.shipInfos.flatMap(p4 =>
        testDB.tables.buyers.map(p5 => (innerK1 = p3.outerK1, innerK2 = p5.name, innerK3 = p4.id))
      )
    )


  def expectedQueryPattern =
    """
      SELECT
        subquery$A.outerK1 as innerK1, buyers$B.name as innerK2, shippingInfo$E.id as innerK3
      FROM
        (SELECT
            purchase$C.id as outerK1, product$D.id as outerK2
         FROM
            purchase as purchase$C,
            product as product$D) as subquery$A,
        shippingInfo as shippingInfo$E,
        buyers as buyers$B
      """
}

class NestedJoinSubquery13Test extends SQLStringQueryTest[AllCommerceDBs, Buyer] {
  def testDescription = "Subquery: flatmap with flatmap on lhs and rhs and identity map"

  def query() =
    val lhs = testDB.tables.purchases.flatMap(p1 =>
      testDB.tables.products.map(p => p)
    )

    lhs.flatMap(p3 =>
      testDB.tables.shipInfos.flatMap(p4 =>
        testDB.tables.buyers.map(b => b)
      )
    )


  def expectedQueryPattern =
    """
      SELECT
        buyers$B
      FROM
        (SELECT
           product$D
         FROM
            purchase as purchase$C,
            product as product$D) as subquery$A,
        shippingInfo as shippingInfo$E,
        buyers as buyers$B
      """
}

class NestedJoinSubquery14Test extends SQLStringQueryTest[AllCommerceDBs, Buyer] {
  def testDescription = "Subquery: flatmap with flatmap on lhs and rhs and no map"

  def query() =
    val lhs = testDB.tables.purchases.flatMap(p1 =>
      testDB.tables.products
    )

    lhs.flatMap(p3 =>
      testDB.tables.shipInfos.flatMap(p4 =>
        testDB.tables.buyers
      )
    )


  def expectedQueryPattern =
    """
      SELECT
        buyers$B
      FROM
        (SELECT
           product$D
         FROM
            purchase as purchase$C,
            product as product$D) as subquery$A,
        shippingInfo as shippingInfo$E,
        buyers as buyers$B
      """
}

// TODO: should map on the outside error?
class NestedJoinSubquery15Test extends SQLStringQueryTest[AllCommerceDBs, Product] {
  def testDescription = "Subquery: flatmap with map on outer table"

  def query() =
    testDB.tables.purchases.map(s => (newId = s.id))
      .flatMap(purch =>
        testDB.tables.products.filter(s => s.name == "test")
      )


  def expectedQueryPattern =
    """
          SELECT product$A
          FROM
            (SELECT purchase$B.id as newId FROM purchase as purchase$B) as subquery$C,
            product as product$A
          WHERE product$A.name = "test"
        """
}

class NestedJoinSubquery16Test extends SQLStringQueryTest[AllCommerceDBs, String] {
  def testDescription = "Subquery: double nested join with project"

  def query() =
    testDB.tables.purchases.flatMap(purch =>
      testDB.tables.products.map(prod =>
        (name = prod.name, id = purch.id).toRow
      )
    ).flatMap(purch =>
      testDB.tables.purchases.flatMap(purch =>
          testDB.tables.products.map(prod =>
            (name = prod.name, id = purch.id).toRow
          )
        )
        //        .sort(_.price, Ord.DESC).take(1)
        .filter(prod => prod.id == purch.id)
        .map(prod => purch.name)
    )


  def expectedQueryPattern =
    """
        SELECT
            subquery$C.name
        FROM
            (SELECT product$B.name as name, purchase$A.id as id FROM purchase as purchase$A, product as product$B) as subquery$C,
            (SELECT product$E.name as name, purchase$D.id as id FROM purchase as purchase$D, product as product$E) as subquery$F
        WHERE subquery$F.id = subquery$C.id
        """
}

class NestedJoinSubqueryFilter1Test extends SQLStringQueryTest[AllCommerceDBs, Buyer] {
  def testDescription = "Subquery: flatmap with flatmap on lhs and rhs and filter on outer"

  def query() =
    val lhs = testDB.tables.purchases.flatMap(p1 =>
      testDB.tables.products.filter(p => p.id > 1)
    )

    lhs.flatMap(p3 =>
      testDB.tables.shipInfos.flatMap(p4 =>
        testDB.tables.buyers
      )
    )


  def expectedQueryPattern =
    """
      SELECT
        buyers$B
      FROM
        (SELECT
           product$D
         FROM
            purchase as purchase$C,
            product as product$D
         WHERE product$D.id > 1) as subquery$A,
        shippingInfo as shippingInfo$E,
        buyers as buyers$B
      """
}

class NestedJoinSubqueryFilter2Test extends SQLStringQueryTest[AllCommerceDBs, Buyer] {
  def testDescription = "Subquery: flatmap with flatmap on lhs and rhs and filter on both outer relations"

  def query() =
    val lhs = testDB.tables.purchases.filter(p => p.id > 2).flatMap(p1 =>
      testDB.tables.products.filter(p => p.id > 1)
    )

    lhs.flatMap(p3 =>
      testDB.tables.shipInfos.flatMap(p4 =>
        testDB.tables.buyers
      )
    )


  def expectedQueryPattern =
    """
      SELECT
        buyers$B
      FROM
        (SELECT
           product$D
         FROM
            purchase as purchase$C,
            product as product$D
         WHERE (purchase$C.id > 2 AND product$D.id > 1)) as subquery$A,
        shippingInfo as shippingInfo$E,
        buyers as buyers$B
      """
}

class NestedJoinSubqueryFilter3Test extends SQLStringQueryTest[AllCommerceDBs, Buyer] {
  def testDescription = "Subquery: flatmap with flatmap on lhs and rhs and filter on both outer relations, one inner relation"

  def query() =
    val lhs = testDB.tables.purchases.filter(p => p.id > 2).flatMap(p1 =>
      testDB.tables.products.filter(p => p.id > 1)
    )

    lhs.flatMap(p3 =>
      testDB.tables.shipInfos.flatMap(p4 =>
        testDB.tables.buyers
      )
    ).filter(b => b.id > 3)

  def expectedQueryPattern =
    """
     SELECT
        *
     FROM
        (SELECT
            buyers$B
         FROM
            (SELECT
                product$D
             FROM
                purchase as purchase$C,
                product as product$D
             WHERE (purchase$C.id > 2 AND product$D.id > 1)) as subquery$A,
            shippingInfo as shippingInfo$E,
            buyers as buyers$B) as subquery$F
         WHERE subquery$F.id > 3
      """

}

class NestedJoinSubqueryFilter4Test extends SQLStringQueryTest[AllCommerceDBs, Buyer] {
  def testDescription = "Subquery: flatmap with flatmap on lhs and rhs and filter on both outer relations, one inner relation"

  def query() =
    val lhs = testDB.tables.purchases.flatMap(p1 =>
      testDB.tables.products
    )

    lhs.flatMap(p3 =>
      testDB.tables.shipInfos.flatMap(p4 =>
        testDB.tables.buyers.filter(b => b.id > 3)
      )
    )

  def expectedQueryPattern =
    """
      SELECT
        buyers$B
      FROM
        (SELECT
           product$D
         FROM
            purchase as purchase$C,
            product as product$D
        shippingInfo as shippingInfo$E,
        buyers as buyers$B
      WHERE buyers$B.id > 3
      """
}
class NestedJoinSubqueryFilter6Test extends SQLStringQueryTest[AllCommerceDBs, Buyer] {
  def testDescription = "Subquery: flatmap with flatmap on lhs and rhs and filter on both outer relations, both inner relation"

  def query() =
    val lhs = testDB.tables.purchases.flatMap(p1 =>
      testDB.tables.products
    )

    lhs.flatMap(p3 =>
      testDB.tables.shipInfos.filter(s => s.id > 4).flatMap(p4 =>
        testDB.tables.buyers.filter(b => b.id > 3)
      )
    )

  def expectedQueryPattern =
    """
      SELECT
        buyers$B
      FROM
        (SELECT
           product$D
         FROM
            purchase as purchase$C,
            product as product$D) as subquery$F
        shippingInfo as shippingInfo$E,
        buyers as buyers$B
      WHERE (shippingInfo$E.id > 4 AND buyers$B.id > 3)
      """
}

class NestedJoinSubqueryFilter7Test extends SQLStringQueryTest[AllCommerceDBs, Buyer] {
  def testDescription = "Subquery: flatmap with flatmap on lhs and rhs and filter outer result"

  def query() =
    val lhs = testDB.tables.purchases.flatMap(p1 =>
      testDB.tables.products
    ).filter(s => s.id > 4)

    lhs.flatMap(p3 =>
      testDB.tables.shipInfos.flatMap(p4 =>
        testDB.tables.buyers
      )
    )

  def expectedQueryPattern =
    """
      SELECT
        buyers$B
      FROM
          (SELECT
             product$D
           FROM
              purchase as purchase$C,
              product as product$D) as subquery$F,
          shippingInfo as shippingInfo$E,
          buyers as buyers$B
      WHERE subquery$F.id > 4
      """
}

class NestedJoinSubqueryFilter8Test extends SQLStringQueryTest[AllCommerceDBs, Buyer] {
  def testDescription = "Subquery: flatmap with flatmap on lhs and rhs filters everywhere"

  def query() =
    val lhs = testDB.tables.purchases.filter(s => s.id > 1).flatMap(p1 =>
      testDB.tables.products.filter(s => s.id > 2)
    ).filter(s => s.id > 3)

    lhs.flatMap(p3 =>
      testDB.tables.shipInfos.filter(s => s.id > 5).flatMap(p4 =>
        testDB.tables.buyers.filter(s => s.id > 6)
      )
    ).filter(s => s.id > 7)

  def expectedQueryPattern =
    """
      SELECT * FROM
        (SELECT
          buyers$B
        FROM
            (SELECT
               product$D
             FROM
                purchase as purchase$C,
                product as product$D
             WHERE (purchase$C.id > 1 AND product$D.id > 2)) as subquery$F,
            shippingInfo as shippingInfo$E,
            buyers as buyers$B
          WHERE (subquery$F.id > 3 AND shippingInfo$E.id > 5 AND buyers$B.id > 6)) as subquery$G
      WHERE subquery$G.id > 7
      """
}

class NestedJoinSubqueryLimit1Test extends SQLStringQueryTest[AllCommerceDBs, Buyer] {
  def testDescription = "Subquery: flatmap with flatmap on lhs and rhs and limit on outer"

  def query() =
    val lhs = testDB.tables.purchases.flatMap(p1 =>
      testDB.tables.products.limit(1)
    )

    lhs.flatMap(p3 =>
      testDB.tables.shipInfos.flatMap(p4 =>
        testDB.tables.buyers
      )
    )


  def expectedQueryPattern =
    """
      SELECT
        buyers$B
      FROM
        (SELECT
           subquery$A
         FROM
            purchase as purchase$C,
            (SELECT * FROM product as product$D LIMIT 1) as subquery$A,
        shippingInfo as shippingInfo$E,
        buyers as buyers$B
      """
}

class NestedJoinSubqueryLimit2Test extends SQLStringQueryTest[AllCommerceDBs, Buyer] {
  def testDescription = "Subquery: flatmap with flatmap on lhs and rhs and limit on both outer relations"

  def query() =
    val lhs = testDB.tables.purchases.limit(2).flatMap(p1 =>
      testDB.tables.products.limit(1)
    )

    lhs.flatMap(p3 =>
      testDB.tables.shipInfos.flatMap(p4 =>
        testDB.tables.buyers
      )
    )


  def expectedQueryPattern =
    """
      SELECT
        buyers$B
      FROM
        (SELECT
           subquery$Y
         FROM
            (SELECT * FROM purchase as purchase$C LIMIT 2) as subquery$X,
            (SELECT * FROM product as product$D LIMIT 1) as subquery$Y) as subquery$Z,
        shippingInfo as shippingInfo$E,
        buyers as buyers$B
      """
}

class NestedJoinSubqueryLimit3Test extends SQLStringQueryTest[AllCommerceDBs, Buyer] {
  def testDescription = "Subquery: flatmap with flatmap on lhs and rhs and limit on both outer relations, one inner relation"

  def query() =
    val lhs = testDB.tables.purchases.limit(2).flatMap(p1 =>
      testDB.tables.products.limit(1)
    )

    lhs.flatMap(p3 =>
      testDB.tables.shipInfos.flatMap(p4 =>
        testDB.tables.buyers
      )
    ).limit(3)

  def expectedQueryPattern =
    """
      SELECT
        buyers$B
      FROM
        (SELECT
           subquery$Y
         FROM
            (SELECT * FROM purchase as purchase$C LIMIT 2) as subquery$X,
            (SELECT * FROM product as product$D LIMIT 1) as subquery$Y) as subquery$Z,
        shippingInfo as shippingInfo$E,
        buyers as buyers$B
      LIMIT 3
      """

}

class NestedJoinSubqueryLimit4Test extends SQLStringQueryTest[AllCommerceDBs, Buyer] {
  def testDescription = "Subquery: flatmap with flatmap on lhs and rhs and limit on both outer relations, one inner relation"

  def query() =
    val lhs = testDB.tables.purchases.flatMap(p1 =>
      testDB.tables.products
    )

    lhs.flatMap(p3 =>
      testDB.tables.shipInfos.flatMap(p4 =>
        testDB.tables.buyers.limit(3)
      )
    )

  def expectedQueryPattern =
    """
      SELECT
        subquery$E
      FROM
        (SELECT
           product$D
         FROM
            purchase as purchase$C,
            product as product$D) as subquery$F,
        shippingInfo as shippingInfo$G,
        (SELECT * FROM buyers as buyers$B LIMIT 3) as subquery$E
      """
}

class NestedJoinSubqueryLimit6Test extends SQLStringQueryTest[AllCommerceDBs, Buyer] {
  def testDescription = "Subquery: flatmap with flatmap on lhs and rhs and limit on both outer relations, both inner relation"

  def query() =
    val lhs = testDB.tables.purchases.flatMap(p1 =>
      testDB.tables.products
    )

    lhs.flatMap(p3 =>
      testDB.tables.shipInfos.limit(4).flatMap(p4 =>
        testDB.tables.buyers.limit(3)
      )
    )

  def expectedQueryPattern =
    """
      SELECT
         subquery$Z
      FROM
        (SELECT
           product$D
         FROM
            purchase as purchase$C,
            product as product$D) as subquery$F
        (SELECT * FROM shippingInfo as shippingInfo$E LIMIT 4) as subquery$X,
        (SELECT * FROM buyers as buyers$B LIMIT 3) as subquery$Z
      """
}

class NestedJoinSubqueryLimit7Test extends SQLStringQueryTest[AllCommerceDBs, Buyer] {
  def testDescription = "Subquery: flatmap with flatmap on lhs and rhs and limit outer result"

  def query() =
    val lhs = testDB.tables.purchases.flatMap(p1 =>
      testDB.tables.products
    ).limit(4)

    lhs.flatMap(p3 =>
      testDB.tables.shipInfos.flatMap(p4 =>
        testDB.tables.buyers
      )
    )

  def expectedQueryPattern =
    """
      SELECT
        buyers$B
      FROM
          (SELECT
             product$D
           FROM
              purchase as purchase$C,
              product as product$D
           LIMIT 4) as subquery$F,
          shippingInfo as shippingInfo$E,
          buyers as buyers$B
      """
}

class NestedJoinSubqueryLimit8Test extends SQLStringQueryTest[AllCommerceDBs, Buyer] {
  def testDescription = "Subquery: flatmap with flatmap on lhs and rhs limits everywhere"

  def query() =
    val lhs = testDB.tables.purchases.limit(1).flatMap(p1 =>
      testDB.tables.products.limit(2)
    ).limit(3)

    lhs.flatMap(p3 =>
      testDB.tables.shipInfos.limit(5).flatMap(p4 =>
        testDB.tables.buyers.limit(6)
      )
    ).limit(7)

  def expectedQueryPattern =
    """
        SELECT
          subquery$I
        FROM
            (SELECT
               subquery$S
             FROM
                (SELECT * FROM purchase as purchase$C LIMIT 1) as subquery$R,
                (SELECT * FROM product as product$D LIMIT 2) as subquery$S
             LIMIT 3) as subquery$P,
            (SELECT * FROM shippingInfo as shippingInfo$E LIMIT 5) as subquery$O,
            (SELECT * FROM buyers as buyers$B LIMIT 6) as subquery$I
        LIMIT 7
      """
}

class NestedJoinSubqueryFilterMap1Test extends SQLStringQueryTest[AllCommerceDBs, (newId: Int)] {
  def testDescription = "Subquery: flatmap with flatmap on lhs and rhs and filterMap on outer"

  def query() =
    val lhs = testDB.tables.purchases.flatMap(p1 =>
      testDB.tables.products.filter(p => p.id > 1).map(r => (newId=r.id))
    )

    lhs.flatMap(p3 =>
      testDB.tables.shipInfos.flatMap(p4 =>
        testDB.tables.buyers.map(r => (newId = r.id))
      )
    )


  def expectedQueryPattern =
    """
      SELECT
        buyers$B.id as newId
      FROM
        (SELECT
           product$D.id as newId
         FROM
            purchase as purchase$C,
            product as product$D
         WHERE product$D.id > 1) as subquery$A,
        shippingInfo as shippingInfo$E,
        buyers as buyers$B
      """
}

class NestedJoinSubqueryFilterMap2Test extends SQLStringQueryTest[AllCommerceDBs, (newId: Int)] {
  def testDescription = "Subquery: flatmap with flatmap on lhs and rhs and filterMap on both outer relations"

  def query() =
    val lhs = testDB.tables.purchases.filter(p => p.id > 2).map(r => (newId=r.id)).flatMap(p1 =>
      testDB.tables.products.filter(p => p.id > 1).map(r => (newId=r.id))
    )

    lhs.flatMap(p3 =>
      testDB.tables.shipInfos.flatMap(p4 =>
        testDB.tables.buyers.map(r => (newId = r.id))
      )
    )


  def expectedQueryPattern =
    """
      SELECT
        buyers$B.id as newId
      FROM
        (SELECT
          product$D.id as newId
        FROM
          (SELECT
              purchase$C.id as newId
            FROM
              purchase as purchase$C
            WHERE purchase$C.id > 2) as subquery$Z,
          product as product$D
        WHERE
          product$D.id > 1) as subquery$A,
       shippingInfo as shippingInfo$E,
       buyers as buyers$B
      """
}

class NestedJoinSubqueryFilterMap3Test extends SQLStringQueryTest[AllCommerceDBs, (newId: Int)] {
  def testDescription = "Subquery: flatmap with flatmap on lhs and rhs and filterMap on both outer relations, one inner relation"

  def query() =
    val lhs = testDB.tables.purchases.filter(p => p.id > 2).map(r => (newId=r.id)).flatMap(p1 =>
      testDB.tables.products.filter(p => p.id > 1).map(r => (newId=r.id))
    )

    lhs.flatMap(p3 =>
      testDB.tables.shipInfos.flatMap(p4 =>
        testDB.tables.buyers.map(r => (newId = r.id))
      )
    ).filter(p=> p.newId > 3).map(r => (newId=r.newId))

  def expectedQueryPattern =
    """
    SELECT
      subquery$F.newId as newId
    FROM
        (SELECT
          buyers$A.id as newId
         FROM
           (SELECT
                product$B.id as newId
             FROM
                (SELECT purchase$C.id as newId FROM purchase as purchase$C WHERE purchase$C.id > 2) as subquery$Z,
                product as product$B
             WHERE product$B.id > 1) as subquery$G,
           shippingInfo as shippingInfo$D,
           buyers as buyers$A) as subquery$F
    WHERE subquery$F.newId > 3
    """

}

class NestedJoinSubqueryFilterMap4Test extends SQLStringQueryTest[AllCommerceDBs, (newId: Int)] {
  def testDescription = "Subquery: flatmap with flatmap on lhs and rhs and filterMap on both outer relations, one inner relation"

  def query() =
    val lhs = testDB.tables.purchases.flatMap(p1 =>
      testDB.tables.products.map(r => (newId = r.id))
    )

    lhs.flatMap(p3 =>
      testDB.tables.shipInfos.flatMap(p4 =>
        testDB.tables.buyers.filter(p=> p.id > 3).map(r => (newId=r.id))
      )
    )

  def expectedQueryPattern =
    """
      SELECT
        buyers$B.id as newId
      FROM
        (SELECT
           product$D.id as newId
         FROM
            purchase as purchase$C,
            product as product$D
        shippingInfo as shippingInfo$E,
        buyers as buyers$B
      WHERE buyers$B.id > 3
      """
}

class NestedJoinSubqueryFilterMap6Test extends SQLStringQueryTest[AllCommerceDBs, (newId: Int)] {
  def testDescription = "Subquery: flatmap with flatmap on lhs and rhs and filterMap on both outer relations, both inner relation"

  def query() =
    val lhs = testDB.tables.purchases.flatMap(p1 =>
      testDB.tables.products.map(r => (newId = r.id))
    )

    lhs.flatMap(p3 =>
      testDB.tables.shipInfos.filter(p => p.id > 4).map(r => (newId=r.id)).flatMap(p4 =>
        testDB.tables.buyers.filter(p =>p.id > 3).map(r => (newId=r.id))
      )
    )

  def expectedQueryPattern =
    """
      SELECT
        buyers$B.id as newId
      FROM
        (SELECT
           product$D.id as newId
         FROM
            purchase as purchase$C,
            product as product$D) as subquery$F
        (SELECT shippingInfo$E.id as newId FROM shippingInfo as shippingInfo$E WHERE shippingInfo$E.id > 4) as subquery$Q,
        buyers as buyers$B
      WHERE buyers$B.id > 3
      """
}

class NestedJoinSubqueryFilterMap7Test extends SQLStringQueryTest[AllCommerceDBs, (newId: Int)] {
  def testDescription = "Subquery: flatmap with flatmap on lhs and rhs and filterMap outer result"

  def query() =
    val lhs = testDB.tables.purchases.flatMap(p1 =>
      testDB.tables.products
    ).filter(p =>p.id > 4).map(r => (newId=r.id))

    lhs.flatMap(p3 =>
      testDB.tables.shipInfos.flatMap(p4 =>
        testDB.tables.buyers.map(r => (newId = r.id))
      )
    )

  def expectedQueryPattern =
    """
      SELECT
        buyers$B.id as newId
      FROM
          (SELECT
             subquery$F.id as newId
           FROM
              (SELECT product$D FROM purchase as purchase$C, product as product$D) as subquery$F
           WHERE subquery$F.id > 4) as subquery$G,
          shippingInfo as shippingInfo$E,
          buyers as buyers$B
      """
}

class NestedJoinSubqueryFilterMap8Test extends SQLStringQueryTest[AllCommerceDBs, (newId: Int)] {
  def testDescription = "Subquery: flatmap with flatmap on lhs and rhs filterMaps everywhere"

  def query() =
    val lhs = testDB.tables.purchases.filter(p =>p.id > 1).map(r => (newId=r.id)).flatMap(p1 =>
      testDB.tables.products.filter(p =>p.id > 2).map(r => (newId=r.id))
    ).filter(p =>p.newId > 3).map(r => (newId=r.newId))

    lhs.flatMap(p3 =>
      testDB.tables.shipInfos.filter(p => p.id > 5).map(r => (newId=r.id)).flatMap(p4 =>
        testDB.tables.buyers.filter(p => p.id > 6).map(r => (newId=r.id))
      )
    ).filter(p =>p.newId > 7).map(r => (newId=r.newId))

  def expectedQueryPattern =
    """
    SELECT
        subquery$A.newId as newId
    FROM
        (SELECT
            buyers$F.id as newId
         FROM
            (SELECT
                subquery$D.newId as newId
             FROM
                (SELECT
                    product$G.id as newId
                 FROM
                    (SELECT
                        purchase$H.id as newId
                     FROM
                        purchase as purchase$H
                     WHERE purchase$H.id > 1) as subquery$E,
                    product as product$G
                 WHERE product$G.id > 2) as subquery$D
             WHERE subquery$D.newId > 3) as subquery$C,
            (SELECT
                shippingInfo$I.id as newId
             FROM
                shippingInfo as shippingInfo$I
             WHERE shippingInfo$I.id > 5) as subquery$B,
            buyers as buyers$F
         WHERE buyers$F.id > 6) as subquery$A
    WHERE subquery$A.newId > 7
      """
}

class NestedJoinExtraTest extends SQLStringQueryTest[AllCommerceDBs, (name: String, id: Int)] {
  def testDescription = "Subquery: nested flatMaps with map from both"

  def query() =
    testDB.tables.purchases.flatMap(purch =>
      testDB.tables.products.map(prod =>
        (name = prod.name, id = purch.id).toRow
      )
    ).filter(row => row.name == "test")


  def expectedQueryPattern =
    """
      SELECT *
      FROM
        (SELECT
            product$A.name as name,
            purchase$B.id as id
        FROM
          purchase as purchase$B,
          product as product$A)
        as subquery$C
      WHERE subquery$C.name = "test"
    """
}

class NestedJoinExtra2Test extends SQLStringQueryTest[AllCommerceDBs, (name: String, id: Int)] {
  def testDescription = "Subquery: nested flatMaps with map from both"

  def query() =
    testDB.tables.purchases.filter(row => row.id == 1).flatMap(purch =>
      testDB.tables.products.filter(row => row.name == "test").map(prod =>
        (name = prod.name, id = purch.id).toRow
      )
    )


  def expectedQueryPattern =
    """
      SELECT
            product$A.name as name,
            purchase$B.id as id
        FROM
          purchase as purchase$B,
          product as product$A
      WHERE (purchase$B.id = 1 AND product$A.name = "test")
    """
}


class NestedJoinExtra4Test extends SQLStringQueryTest[AllCommerceDBs, Purchase] {
  def testDescription = "Subquery: nested join with one side nested"

  def query() =
    val table1 = testDB.tables.purchases.flatMap(purch =>
      testDB.tables.products.map(prod =>
        (name = prod.name, id = purch.id).toRow
      )
    )
    table1.flatMap(t =>
      testDB.tables.purchases.filter(f => f.id == t.id)
    )


  def expectedQueryPattern =
    """
    SELECT
      purchase$D
    FROM
      (SELECT
          product$B.name as name, purchase$A.id as id
       FROM
          purchase as purchase$A,
          product as product$B) as subquery$C,
      purchase as purchase$D
    WHERE purchase$D.id = subquery$C.id
    """
}


class SubqueryInFilterSubqueryTest extends SQLStringQueryTest[AllCommerceDBs, Buyer] {
  def testDescription = "Subquery: subqueryInFilter"
  def query() =
    testDB.tables.buyers.filter(c =>
      testDB.tables.shipInfos.filter(s =>
        c.id == s.buyerId
      ).size == 0
    )
  def expectedQueryPattern = """
        SELECT
          *
        FROM buyers as buyers$A
        WHERE
          (SELECT
            COUNT(1)
            FROM shippingInfo as shippingInfo$B
            WHERE buyers$A.id = shippingInfo$B.buyerId) = 0
      """
}
class SubqueryInMapSubqueryTest extends SQLStringQueryTest[AllCommerceDBs, (buyers: Buyer, count: Int)] {
  def testDescription = "Subquery: subqueryInMap"
  def query() =
    testDB.tables.buyers.map(c =>
      (buyers = c, count = testDB.tables.shipInfos.filter(p => c.id == p.buyerId).size).toRow
    )
  def expectedQueryPattern = """
        SELECT buyers$A as buyers, (SELECT COUNT(1) FROM shippingInfo as shippingInfo$B WHERE buyers$A.id = shippingInfo$B.buyerId) as count FROM buyers as buyers$A
      """
}
class SubqueryInMapNestedSubqueryTest extends SQLStringQueryTest[AllCommerceDBs, (buyer: Buyer, occurances: Boolean)] {
  def testDescription = "Subquery: subqueryInMapNested"
  def query() =
    testDB.tables.buyers.map(c =>
      (buyer = c, occurances = testDB.tables.shipInfos.filter(p => p.buyerId == c.id).size == 1).toRow
    )
  def expectedQueryPattern = """
        SELECT
          buyers$B as buyer,
           (SELECT
              COUNT(1)
           FROM shippingInfo as shippingInfo$A
           WHERE shippingInfo$C.buyerId = buyers$B.id) = 1 as occurances
        FROM
          buyers as buyers$B
      """
}
class SubqueryInMapNestedConcatSubqueryTest extends SQLStringQueryTest[AllCommerceDBs, (id: Int, name: String, dateOfBirth: LocalDate, occurances: Int)] {
  def testDescription = "Subquery: subqueryInMapNested"
  def query() =
    testDB.tables.buyers.map(c =>
      (id = c.id, name = c.name, dateOfBirth = c.dateOfBirth).toRow.concat((occurances = testDB.tables.shipInfos.filter(p => p.buyerId == c.id).size))
    )
  def expectedQueryPattern = """
        SELECT
          buyers$B.id as id, buyers$B.name as name, buyers$B.dateOfBirth as dateOfBirth , (SELECT
            COUNT(1)
            FROM shippingInfo as shippingInfo$C
            WHERE shippingInfo$C.buyerId = buyers$B.id) as occurances
        FROM
          buyers as buyers$B
      """
}

class SubqueryLimitUnionSelectSubqueryTest extends SQLStringQueryTest[AllCommerceDBs, String] {
  def testDescription = "Subquery: LimitUnionSelect"
  def query() =
    testDB.tables.buyers
      .map(_.name.toLowerCase)
      .take(2)
      .unionAll(
        testDB.tables.products
          .map(_.name.toLowerCase)
      )
  def expectedQueryPattern = """
        SELECT LOWER(buyers$A.name) FROM buyers as buyers$A LIMIT 2
        UNION ALL
        SELECT LOWER(product$B.name) FROM product as product$B
      """
}

class SubqueryUnionSelectLimitSubqueryTest extends SQLStringQueryTest[AllCommerceDBs, String] {
  def testDescription = "Subquery: UnionSelectLimit"
  def query() =
    testDB.tables.buyers
      .map(_.name.toLowerCase)
      .unionAll(
        testDB.tables.products
          .map(_.name.toLowerCase)
          .take(2))
  def expectedQueryPattern = """
        SELECT LOWER(buyers$A.name) FROM buyers as buyers$A
        UNION ALL
        SELECT LOWER(product$B.name) FROM product as product$B LIMIT 2
      """
}
class ExceptAggregateSubqueryTest extends SQLStringQueryTest[AllCommerceDBs, (max: Double, min: Double)] {
  def testDescription = "Subquery: exceptAggregate"
  def query() =
    testDB.tables.products
      .map(p => (name = p.name.toLowerCase, price = p.price))
      .except(
        testDB.tables.products
          .map(p => (name = p.name.toLowerCase, price = p.price))
      )
      .map(ps => (max = max(ps.price), min = min(ps.price)))
  def expectedQueryPattern = """
     SELECT MAX(subquery$E.price) as max, MIN(subquery$E.price) as min FROM
        (SELECT LOWER(product$A.name) as name, product$A.price as price FROM product as product$A
        EXCEPT
        SELECT LOWER(product$B.name) as name, product$B.price as price FROM product as product$B) as subquery$E
      """
}

class UnionAllAggregateSubqueryTest extends SQLStringQueryTest[AllCommerceDBs, (max: Double, min: Double)] {
  def testDescription = "Subquery: unionAllAggregate"
  def query() =
    testDB.tables.products
      .map(p => (name = p.name.toLowerCase, price = p.price))
      .unionAll(testDB.tables.products
        .map(p2 => (name = p2.name.toLowerCase, price = p2.price))
      )
      .map(p => (max = max(p.price), min = min(p.price)))
  def expectedQueryPattern = """
        SELECT
          MAX(subquery$A.price) as max,
          MIN(subquery$A.price) as min
        FROM
          (SELECT
              LOWER(product$B.name) as name, product$B.price as price
           FROM
              product as product$B
           UNION ALL
            SELECT
              LOWER(product$C.name) as name, product$C.price as price
            FROM
              product as product$C) as subquery$A
      """
}
