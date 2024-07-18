package test.query.selectmodifiers
import test.SQLStringQueryTest
import test.query.{commerceDBs,  AllCommerceDBs}

import tyql.*
import tyql.Expr.toRow
import language.experimental.namedTuples
import NamedTuple.*
import scala.language.implicitConversions

class SelectModifiersRepeatSelectTest extends SQLStringQueryTest[AllCommerceDBs, (name3: String, id3: Int)] {
  def testDescription: String = "SelectModifiers: double map on temporary"
  def query() =
    val q = testDB.tables.products.map: prod =>
      (name2 = prod.name, id2 = prod.id).toRow
    q.map: prod =>
      (name3 = prod.name2, id3 = prod.id2).toRow
  def expectedQueryPattern: String =  "SELECT subquery$A.name2 as name3, subquery$A.id2 as id3 FROM (SELECT product$B.name as name2, product$B.id as id2 FROM product as product$B) as subquery$A"
}
// should be exactly the same as above
class SelectModifiersRepeatSelect2Test extends SQLStringQueryTest[AllCommerceDBs, (name3: String, id3: Int)] {
  def testDescription: String = "SelectModifiers: double map fluent"
  def query() =
    testDB.tables.products.map: prod =>
      (name2 = prod.name, id2 = prod.id).toRow
    .map: prod =>
      (name3 = prod.name2, id3 = prod.id2).toRow
  def expectedQueryPattern: String =  "SELECT subquery$A.name2 as name3, subquery$A.id2 as id3 FROM (SELECT product$B.name as name2, product$B.id as id2 FROM product as product$B) as subquery$A"
}
class SelectModifiersSortTest extends SQLStringQueryTest[AllCommerceDBs, (name: String)] {
  def testDescription: String = "SelectModifiers: sort"
  def query() =
    testDB.tables.products
      // .sort: prod => (prod.id = Ord.ASC, ...) // alternative structure
      .sort(_.id, Ord.ASC)
      .map: prod =>
        (name = prod.name).toRow
  def expectedQueryPattern: String =  "SELECT product$A.name as name FROM product as product$A ORDER BY product$A.id ASC"
}

class SelectModifiersSort2Test extends SQLStringQueryTest[AllCommerceDBs, (name: String)] {
  def testDescription: String = "SelectModifiers: sort with 2 keys"
  def query() =
    testDB.tables.products
      // .sort: prod => (prod.id = Ord.ASC, prod.price = Ord.DESC) // alternative structure
      .sort(_.id, Ord.ASC)
      .sort(_.price, Ord.DESC)
      .sort(_.name, Ord.DESC)
      .map: prod =>
        (name = prod.name).toRow
  def expectedQueryPattern: String =  "SELECT product$A.name as name FROM product as product$A ORDER BY product$A.id ASC, product$A.price DESC, product$A.name DESC"
}

class SelectModifiersSort3Test extends SQLStringQueryTest[AllCommerceDBs, (name2: String)] {
  def testDescription: String = "SelectModifiers: sort on new name"
  def query() =
    testDB.tables.products
      .map: prod =>
        (name2 = prod.name).toRow
      .sort(_.name2, Ord.DESC)
  def expectedQueryPattern: String =  "SELECT * FROM (SELECT product$A.name as name2 FROM product as product$A) as subquery$B ORDER BY subquery$B.name2 DESC"
}

class SelectModifiersSortlimitSelectTest extends SQLStringQueryTest[AllCommerceDBs, (name: String)] {
  def testDescription: String = "SelectModifiers: sortLimit"
  def query() =
    testDB.tables.products
      .sort(_.id, Ord.ASC)
      .map: prod =>
        (name = prod.name).toRow
      .limit(3)
  def expectedQueryPattern: String =  "SELECT product$A.name as name FROM product as product$A ORDER BY product$A.id ASC LIMIT 3"
}

class SelectModifiersSortoffsetSelectTest extends SQLStringQueryTest[AllCommerceDBs, (name: String)] {
  def testDescription: String = "SelectModifiers: sortOffset"
  def query() =
    testDB.tables.products
      .sort(_.id, Ord.ASC)
      .map: prod =>
        (name = prod.name).toRow
      .drop(2)
  def expectedQueryPattern: String =  "SELECT product$A.name as name FROM product as product$A ORDER BY product$A.id ASC OFFSET 2" // TODO: offset in other DBs
}

class SelectModifiersSortlimittwiceSelectTest extends SQLStringQueryTest[AllCommerceDBs, (name: String)] {
  def testDescription: String = "SelectModifiers: sortLimitTwice"
  def query() =
    testDB.tables.products
      .sort(_.id, Ord.ASC)
      .map: prod =>
        (name = prod.name).toRow
      .drop(2)
      .take(4)
  def expectedQueryPattern: String =  "SELECT product$A.name as name FROM product as product$A ORDER BY product$A.id ASC OFFSET 2 LIMIT 4"
}

class SelectModifiersDistinctSelectTest extends SQLStringQueryTest[AllCommerceDBs, (name: String)] {
  def testDescription: String = "SelectModifiers: distinct"
  def query() =
    testDB.tables.products
      .sort(_.price, Ord.ASC)
      .map: prod =>
        (name = prod.name).toRow
      .distinct
  def expectedQueryPattern: String =  "SELECT DISTINCT product$A.name as name FROM product as product$A ORDER BY product$A.price ASC"
}

// TODO: inline subqueries
//class SelectModifiersNestSortSelectTest extends SQLStringQueryTest[AllCommerceDBs, (name: String, id: Int)] {
//  def testDescription: String = "SelectModifiers: sort before join"
//  def query() =
//    for
//      prod <- testDB.tables.products.sort(_.id, Ord.ASC).map(prod => (name2 = prod.name, id = prod.id).toRow)
//      purch <- testDB.tables.purchases
//      if prod.id == purch.id
//    yield (name = prod.name2, id = purch.id).toRow
//
//  def expectedQueryPattern: String =  """
//    SELECT product1.name AS res
//      FROM (SELECT purchase0.product_id AS product_id, purchase0.total AS total
//        FROM purchase purchase0
//        ORDER BY total DESC
//        LIMIT ?) subquery0
//      CROSS JOIN product product1
//      WHERE (product1.id = subquery0.product_id)
//    """
//}
//
//class SelectModifiersFlatmapSelectTest extends SQLStringQueryTest[AllCommerceDBs, (id1: Int, id: Int)] {
//  def testDescription: String = "SelectModifiers: flatMap"
//  def query() =
//    for
//      prod <- testDB.tables.products.sort(_.id, Ord.ASC).map(prod => prod.id)
//      purch <- testDB.tables.purchases
//      if prod == purch.id
//    yield (id1 = purch.id, id = prod).toRow
//
//  def expectedQueryPattern: String =  """
//    SELECT product1.name AS res
//      FROM (SELECT purchase0.product_id AS product_id, purchase0.total AS total
//        FROM purchase purchase0
//        ORDER BY total DESC
//        LIMIT ?) subquery0
//      CROSS JOIN product product1
//      WHERE (product1.id = subquery0.product_id)
//    """
//}