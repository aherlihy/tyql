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
        purchase as purchase$D,
        (SELECT *
          FROM product as product$B
          ORDER BY price DESC
          LIMIT 1) as subquery$C
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
          FROM product as product$B
          ORDER BY price DESC
          LIMIT 1) as subquery$D,
         (SELECT *
          FROM purchase as purchase$A
          ORDER BY total ASC
          LIMIT 2) as subquery$E
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
           FROM product as product$A
           ORDER BY price DESC) as subquery$B,
          (SELECT *
           FROM
           purchase as purchase$D
           ORDER BY total ASC) as subquery$C
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
          product as product$A,
          (SELECT * FROM purchase as purchase$C OFFSET 1) as subquery$B
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
        purchase as purchase$D,
        (SELECT *
          FROM product as product$B
          ORDER BY price DESC
          LIMIT 1) as subquery$C
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
        (SELECT * FROM purchase as purchase$R ORDER BY count DESC LIMIT 4) as subquery$B,
        (SELECT * FROM product as product$Q ORDER BY price DESC LIMIT 3) as subquery$A
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

//class NestedJoinSubqueryTest extends SQLStringQueryTest[AllCommerceDBs, String] {
//  def testDescription = "Subquery: double nested join with project"
//
//  def query() =
//    testDB.tables.purchases.flatMap(purch =>
//      testDB.tables.products.map(prod =>
//        (name = prod.name, id = purch.id).toRow
//      )
//    ).flatMap(purch =>
//      testDB.tables.purchases.flatMap(purch =>
//          testDB.tables.products.map(prod =>
//            (name = prod.name, id = purch.id).toRow
//          )
//        )
//        //        .sort(_.price, Ord.DESC).take(1)
//        .filter(prod => prod.id == purch.id)
//        .map(prod => purch.name)
//    )
//
//
//  def expectedQueryPattern =
//    """
//      SELECT subquery$S.name
//      FROM
//        (SELECT product$A.name as name, purchase$B.id as id FROM purchase as purchase$B, product as product$A) as subquery$S,
//        (SELECT product$X.name as name, purchase$Y.id as id FROM purchase as purchase$Y, product as product$X) as subquery$R
//      WHERE
//        subquery$S.id = subquery$R.id
//
//      SELECT subquery83.name
//      FROM
//        (SELECT product80.name as name, purchase79.id as id FROM purchase as purchase79, product as product80,
//         (SELECT product85.name as name, purchase84.id as id FROM purchase as purchase84, product as product85) as subquery88
//      WHERE
//        subquery88.id = subquery83.id) as subquery89
//    """
//}

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

class NestedJoinSubquery3Test extends SQLStringQueryTest[AllCommerceDBs, Purchase] {
  def testDescription = "Subquery: nested join with one side nested"

  def query() =
    val table1 = testDB.tables.purchases.flatMap(purch =>
      testDB.tables.products.map(prod =>
        (name = prod.name, id = purch.id).toRow
      )
    )
    table1.flatMap(t =>
      testDB.tables.purchases.map(s => s).filter(f => f.id == t.id)
    )


  def expectedQueryPattern =
    /**
     * TODO: right now we don't hoist the join condition because the assumption is that a filter after a map should force
     * a subquery because the map could change the types entirely. Revisit assumption and see if we can drop it so there
     * is no further nesting.
     */
    """
        SELECT subquery220.prodId
        FROM
          ( SELECT product217.id as prodId
            FROM
              purchase as purchase216,
              product as product217,
              (SELECT product222.id as prodId FROM purchase as purchase221, product as product222) as subquery225
            WHERE subquery225.prodId = subquery220.prodId) as subquery226
    """
}
//class sortLimitSortLimitSubqueryTest extends SQLStringQueryTest[AllCommerceDBs, String] {
//  def testDescription = "Subquery: sortLimitSortLimit"
//  def query() =
//    testDB.tables.products.sort(_.price, Ord.DESC).take(4).sort(_.price, Ord.DESC).take(2).map(_.name)
//  def expectedQueryPattern = """
//        SELECT subquery0.name AS res
//        FROM (SELECT
//            product0.name AS name,
//            product0.price AS price
//          FROM product product0
//          ORDER BY price DESC
//          LIMIT ?) subquery0
//        ORDER BY subquery0.price ASC
//        LIMIT ?
//      """
//}
//class subqueryInFilterSubqueryTest extends SQLStringQueryTest[AllCommerceDBs, Buyer] {
//  def testDescription = "Subquery: subqueryInFilter"
//  def query() =
//    testDB.tables.buyers.filter(c =>
//      testDB.tables.shipInfos.filter(s =>
//        c.id == s.buyerId
//      ).size == 0
//    )
//  def expectedQueryPattern = """
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
//class SubqueryInMapSubqueryTest extends SQLStringQueryTest[AllCommerceDBs, (buyer: Buyer, count: Int)] {
//  def testDescription = "Subquery: subqueryInMap"
//  def query() =
//    testDB.tables.buyers.map(c =>
//      (buyer = c, count = testDB.tables.shipInfos.filter(p => c.id == p.buyerId).size).toRow
//    )
//  def expectedQueryPattern = """
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
//class subqueryInMapNestedSubqueryTest extends SQLStringQueryTest[AllCommerceDBs, (buyer: Buyer, occurances: Boolean)] {
//  def testDescription = "Subquery: subqueryInMapNested"
//  def query() =
//    testDB.tables.buyers.map(c =>
//      (buyer = c, occurances = testDB.tables.shipInfos.filter(p => p.buyerId == c.id).size == 1).toRow
//    )
//  def expectedQueryPattern = """
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
//class subqueryInMapNestedConcatSubqueryTest extends SQLStringQueryTest[AllCommerceDBs, (id: Int, name: String, dateOfBirth: LocalDate, occurances: Int)] {
//  def testDescription = "Subquery: subqueryInMapNested"
//  def query() =
//    testDB.tables.buyers.map(c =>
//      (id = c.id, name = c.name, dateOfBirth = c.dateOfBirth).toRow.concat((occurances = testDB.tables.shipInfos.filter(p => p.buyerId == c.id).size))
//    )
//  def expectedQueryPattern = """
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
//
//class selectLimitUnionSelectSubqueryTest extends SQLStringQueryTest[AllCommerceDBs, String] {
//  def testDescription = "Subquery: selectLimitUnionSelect"
//  def query() =
//    testDB.tables.buyers.map(_.name.toLowerCase).take(2).unionAll(testDB.tables.products.map(_.name.toLowerCase))
//  def expectedQueryPattern = """
//        SELECT subquery0.res AS res
//        FROM (SELECT
//            LOWER(buyer0.name) AS res
//          FROM buyer buyer0
//          LIMIT ?) subquery0
//        UNION ALL
//        SELECT LOWER(product0.kebab_case_name) AS res
//        FROM product product0
//      """
//}
//
//class selectUnionSelectLimitSubqueryTest extends SQLStringQueryTest[AllCommerceDBs, String] {
//  def testDescription = "Subquery: selectUnionSelectLimit"
//  def query() =
//    testDB.tables.buyers.map(_.name.toLowerCase).unionAll(testDB.tables.products.map(_.name.toLowerCase).take(2))
//  def expectedQueryPattern = """
//        SELECT LOWER(buyer0.name) AS res
//        FROM buyer buyer0
//        UNION ALL
//        SELECT subquery0.res AS res
//        FROM (SELECT
//            LOWER(product0.kebab_case_name) AS res
//          FROM product product0
//          LIMIT ?) subquery0
//      """
//}
//class ExceptAggregateSubqueryTest extends SQLStringQueryTest[AllCommerceDBs, (max: Double, min: Double)] {
//  def testDescription = "Subquery: exceptAggregate"
//  def query() =
//    import Aggregation.toRow as AggrToRow
//    testDB.tables.products
//      .map(p => (name = p.name.toLowerCase, price = p.price).toRow)
//      .except(
//        testDB.tables.products.map(p => (name = p.name.toLowerCase, price = p.price).toRow)
//      )
//      .map(ps => (max = max(ps.price), min = min(ps.price)).AggrToRow)
//  def expectedQueryPattern = """
//        SELECT
//          MAX(subquery0.res_1) AS res_0,
//          MIN(subquery0.res_1) AS res_1
//        FROM (SELECT
//            LOWER(product0.name) AS res_0,
//            product0.price AS res_1
//          FROM product product0
//          EXCEPT
//          SELECT
//            LOWER(product0.kebab_case_name) AS res_0,
//            product0.price AS res_1
//          FROM product product0) subquery0
//      """
//}
//
//class UnionAllAggregateSubqueryTest extends SQLStringQueryTest[AllCommerceDBs, (max: Double, min: Double)] {
//  def testDescription = "Subquery: unionAllAggregate"
//  def query() =
//    import Aggregation.toRow as AggrToRow
//    testDB.tables.products
//      .map(p => (name = p.name.toLowerCase, price = p.price).toRow)
//      .unionAll(testDB.tables.products
//        .map(p2 => (name = p2.name.toLowerCase, price = p2.price).toRow)
//      )
//      .map(p => (max = max(p.price), min = min(p.price)).AggrToRow)
//  def expectedQueryPattern = """
//        SELECT
//          MAX(subquery0.res_1) AS res_0,
//          MIN(subquery0.res_1) AS res_1
//        FROM (SELECT product0.price AS res_1
//          FROM product product0
//          UNION ALL
//          SELECT product0.price AS res_1
//          FROM product product0) subquery0
//      """
//}
