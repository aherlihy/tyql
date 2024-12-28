package test.query.selectmodifiers
import test.SQLStringQueryTest
import test.query.{commerceDBs,  AllCommerceDBs, Product}

import tyql.*
import tyql.Expr.toRow
import language.experimental.namedTuples
import NamedTuple.*
import scala.language.implicitConversions

class SelectModifiersRepeatTest extends SQLStringQueryTest[AllCommerceDBs, (name3: String, id3: Int)] {
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
class SelectModifiersSort1Test extends SQLStringQueryTest[AllCommerceDBs, (name: String)] {
  def testDescription: String = "SelectModifiers: sort first, subquery"
  def query() =
    testDB.tables.products
      // .sort: prod => (prod.id = Ord.ASC, ...) // alternative structure
      .sort(_.id, Ord.ASC)
      .map: prod =>
        (name = prod.name).toRow
  def expectedQueryPattern: String = "SELECT subquery$B.name as name FROM (SELECT * FROM product as product$A ORDER BY id ASC) as subquery$B"
}

class SelectModifiersSort1aTest extends SQLStringQueryTest[AllCommerceDBs, (name2: String)] {
  def testDescription: String = "SelectModifiers: sort after map, no subquery"

  def query() =
    testDB.tables.products
      .map: prod =>
        (name2 = prod.name).toRow
      // .sort: prod => (prod.id = Ord.ASC, ...) // alternative structure
      .sort(_.name2, Ord.ASC)

  def expectedQueryPattern: String =  "SELECT product$A.name as name2 FROM product as product$A ORDER BY name2 ASC"
}

class SelectModifiersSort1bTest extends SQLStringQueryTest[AllCommerceDBs, (name3: String)] {
  def testDescription: String = "SelectModifiers: sort before+after map"

  def query() =
    testDB.tables.products
      .map: prod =>
        (name2 = prod.name).toRow
      .sort(_.name2, Ord.ASC)
      .map: prod =>
          (name3 = prod.name2).toRow
      .sort(_.name3, Ord.ASC)

  def expectedQueryPattern: String = "SELECT subquery$B.name2 as name3 FROM (SELECT product$A.name as name2 FROM product as product$A ORDER BY name2 ASC) as subquery$B ORDER BY name3 ASC"
}

class SelectModifiersSort1cTest extends SQLStringQueryTest[AllCommerceDBs, Product] {
  def testDescription: String = "SelectModifiers: sort on table"

  def query() =
    testDB.tables.products
      .sort(_.id, Ord.ASC)

  def expectedQueryPattern: String = "SELECT * FROM product as product$A ORDER BY id ASC"
}


class SelectModifiersLimit1cTest extends SQLStringQueryTest[AllCommerceDBs, Product] {
  def testDescription: String = "SelectModifiers: limit on table"

  def query() =
    testDB.tables.products
      .limit(1)

  def expectedQueryPattern: String = "SELECT * FROM product as product$A LIMIT 1"
}

class SelectModifiersOffset1cTest extends SQLStringQueryTest[AllCommerceDBs, Product] {
  def testDescription: String = "SelectModifiers: offset on table"

  def query() =
    testDB.tables.products
      .offset(1)

  def expectedQueryPattern: String = "SELECT * FROM product as product$A OFFSET 1"
}

class SelectModifiersSort2Test extends SQLStringQueryTest[AllCommerceDBs, (name: String)] {
  def testDescription: String = "SelectModifiers: sort with 3 keys, subquery"
  def query() =
    testDB.tables.products
      // .sort: prod => (prod.id = Ord.ASC, prod.price = Ord.DESC) // alternative structure
      .sort(_.id, Ord.ASC)
      .sort(_.price, Ord.DESC)
      .sort(_.name, Ord.DESC)
      .map: prod =>
        (name = prod.name).toRow
  def expectedQueryPattern: String =  "SELECT subquery$B.name as name FROM (SELECT * FROM product as product$A ORDER BY name DESC, price DESC, id ASC) as subquery$B"
}

class SelectModifiersSort2bTest extends SQLStringQueryTest[AllCommerceDBs, (name2: String, id2: Int, price2: Double)] {
  def testDescription: String = "SelectModifiers: sort with 3 keys, no subquery"

  def query() =
    testDB.tables.products
      .map: prod =>
        (name2 = prod.name, id2 = prod.id, price2 = prod.price).toRow
      .sort(_.id2, Ord.ASC)
      .sort(_.price2, Ord.DESC)
      .sort(_.name2, Ord.DESC)

  def expectedQueryPattern: String = "SELECT product$A.name as name2, product$A.id as id2, product$A.price as price2 FROM product as product$A ORDER BY name2 DESC, price2 DESC, id2 ASC"
}

class SelectModifiersSortLimitTest extends SQLStringQueryTest[AllCommerceDBs, (name: String)] {
  def testDescription: String = "SelectModifiers: sortLimit"
  def query() =
    testDB.tables.products
      .map: prod =>
        (name = prod.name).toRow
      .sort(_.name, Ord.ASC)
      .limit(3)
  def expectedQueryPattern: String =  "SELECT product$A.name as name FROM product as product$A ORDER BY name ASC LIMIT 3"
}

class SelectModifiersSortOffsetTest extends SQLStringQueryTest[AllCommerceDBs, (name: String, id: Int)] {
  def testDescription: String = "SelectModifiers: sortOffset"
  def query() =
    testDB.tables.products
      .map: prod =>
        (name = prod.name, id = prod.id).toRow
      .sort(_.id, Ord.ASC)
      .offset(2)
  def expectedQueryPattern: String =  "SELECT product$A.name as name, product$A.id as id FROM product as product$A ORDER BY id ASC OFFSET 2"
}

class SelectModifiersSortLimit2Test extends SQLStringQueryTest[AllCommerceDBs, (name: String)] {
  def testDescription: String = "SelectModifiers: sortLimitTwice"
  def query() =
    testDB.tables.products
      .map: prod =>
        (name = prod.name).toRow
      .sort(_.name, Ord.ASC)
      .offset(2)
      .take(4)
  def expectedQueryPattern: String =  "SELECT product$A.name as name FROM product as product$A ORDER BY name ASC OFFSET 2 LIMIT 4"
}

class SelectModifiersDistinctTableTest extends SQLStringQueryTest[AllCommerceDBs, Product] {
  def testDescription: String = "SelectModifiers: distinct on Table"

  def query() =
    testDB.tables.products
      .distinct

  def expectedQueryPattern: String = "SELECT DISTINCT * FROM product as product$A"
}

class SelectModifiersDistinctOrderbyTest extends SQLStringQueryTest[AllCommerceDBs, (name: String)] {
  def testDescription: String = "SelectModifiers: distinct on OrderBy"
  def query() =
    testDB.tables.products
      .map: prod =>
        (name = prod.name).toRow
      .sort(_.name, Ord.ASC)
      .distinct
  def expectedQueryPattern: String =  "SELECT DISTINCT product$A.name as name FROM product as product$A ORDER BY name ASC"
}
class SelectModifiersDistinctSelectAllTest extends SQLStringQueryTest[AllCommerceDBs, Product] {
  def testDescription: String = "SelectModifiers: distinct on SelectAll"
  def query() =
    testDB.tables.products.filter(p => p.id > 1)
      .distinct
  def expectedQueryPattern: String =  "SELECT DISTINCT * FROM product as product$A WHERE product$A.id > 1"
}
class SelectModifiersDistinctSelectTest extends SQLStringQueryTest[AllCommerceDBs, (name: String)] {
  def testDescription: String = "SelectModifiers: distinct on Select"
  def query() =
    testDB.tables.products
      .map: prod =>
        (name = prod.name).toRow
      .distinct
  def expectedQueryPattern: String =  "SELECT DISTINCT product$A.name as name FROM product as product$A"
}

class SelectModifiersDistinctNaryTest extends SQLStringQueryTest[AllCommerceDBs, (name: String)] {
  def testDescription: String = "SelectModifiers: distinct on Nary"
  def query() =
    testDB.tables.products
      .map: prod =>
        (name = prod.name).toRow
      .unionAll(testDB.tables.buyers.map(b => (name = b.name).toRow))
      .distinct
  def expectedQueryPattern: String =  "SELECT DISTINCT * FROM ((SELECT product$A.name as name FROM product as product$A) UNION ALL (SELECT buyers$B.name as name FROM buyers as buyers$B)) as subquery$C"
}