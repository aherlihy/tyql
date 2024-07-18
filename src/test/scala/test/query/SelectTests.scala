package test.query.select
import test.{SQLStringQueryTest, TestDatabase}
import test.query.{commerceDBs,  AllCommerceDBs, Product}

import tyql.*
import language.experimental.namedTuples
import NamedTuple.*
import scala.language.implicitConversions

case class CityT(zipCode: Int, name: String, population: Int)
type AddressT = (city: CityT, street: String, number: Int)

type AllLocDBs = (cities: CityT, addresses: AddressT, cities2: CityT)
type CityDB = (cities: CityT)

given cityDB: TestDatabase[CityDB] with
  override def tables = (
    cities = Table[CityT]("cities")
    )

given TestDatabase[AllLocDBs] with
  override def tables = (
    cities = Table[CityT]("cities"),
    addresses = Table[AddressT]("addresses"),
    cities2 = Table[CityT]("cities2")
  )

class SimpleSelectTest extends SQLStringQueryTest[CityDB, Int] {
  def testDescription: String = "Select: Simple select field"
  def query() =
    testDB.tables.cities.map: c =>
      c.zipCode
  def expectedQueryPattern: String = "SELECT cities$A.zipCode FROM cities as cities$A"
}

class SelectWithFilterTest extends SQLStringQueryTest[CityDB, String] {
  def testDescription: String = "Select: select with > filter"
  def query() =
    testDB.tables.cities.withFilter: city =>
      city.population > 10_000
    .map: city =>
      city.name
  def expectedQueryPattern: String = "SELECT cities$A.name FROM cities as cities$A WHERE cities$A.population > 10000"
}

class SelectWithGtTest extends SQLStringQueryTest[CityDB, String] {
  def testDescription: String = "Select: select with gt constraint"
  def query() =
    for
      city <- testDB.tables.cities if city.population > 10_000
    yield city.name
  def expectedQueryPattern: String = "SELECT cities$A.name FROM cities as cities$A WHERE cities$A.population > 10000"
}
/*
class SelectWithSelfNestTest extends SQLStringQueryTest[CityDB, CityT] {
  def testDescription: String = "Select: self-join with condition"
  def query() =
    for
      city <- testDB.tables.cities
      alt <- testDB.tables.cities
      if city.name == alt.name && city.zipCode != alt.zipCode
    yield
      city
  def expectedQueryPattern: String = "SELECT * FROM cities AS cities$A JOIN cities AS alt ON city.name=alt.name AND city.zipcode != alt.zipcode"
}

class SelectNested extends SQLStringQueryTest[AllLocDBs, CityT] {
  def testDescription: String = "Select: two-table join with condition "
  def query() =
    for
      city <- testDB.tables.cities
      address <- testDB.tables.addresses
      if city == address.city
    yield
      city
  def expectedQueryPattern: String =  "SELECT city.* FROM cities AS city JOIN addresses AS address ON city=address.city"
}
*/
class SelectWithProjectTestToRow extends SQLStringQueryTest[CityDB, (name: String, zipCode: Int)] {
  def testDescription: String = "Select: select with project"
  def query() =
    testDB.tables.cities.map: city =>
      (name = city.name, zipCode = city.zipCode).toRow
  def expectedQueryPattern: String = "SELECT cities$A.name as name, cities$A.zipCode as zipCode FROM cities as cities$A"
}

class SelectTableTest extends SQLStringQueryTest[AllCommerceDBs, Product] {
  def testDescription: String = "Select: table"
  def query() =
    testDB.tables.products
  def expectedQueryPattern: String = "SELECT * FROM product as product$A"
}

class SelectMultipleFilterTest extends SQLStringQueryTest[AllCommerceDBs, Product] {
  def testDescription: String = "Select: multiple filter"
  def query() =
    testDB.tables.products
      .filter(p => p.price == 0)
      .filter(p => p.id > 0)
  def expectedQueryPattern: String = "SELECT * FROM product as product$A WHERE product$A.id > 0 AND product$A.price = 0"
}

/*
// TODO: probably use take not single
 class SelectSingleTest extends SQLStringQueryTest[AllCommerceDBs, Product] {
   def testDescription: String = "Select: single"
   def query() =
     testDB.tables.products
       .filter(p => p.price == 0)
       .single
   def expectedQueryPattern: String = """
     SELECT * FROM products
     WHERE price == 0 AND id > 0
     LIMIT 1
     """
 }

// TODO: determine what contains, nonempty, and isEmpty syntax makes the most sense
class ContainsTest extends SQLStringQueryTest[AllCommerceDBs, Product] {
  def testDescription: String = "Contains"
  def query() =
    testDB.tables.products
      .filter(p =>
        testDB.tables.purchases.map(
          pu => (id = pu.id).toRow
        ).contains(
          (id = p.id).toRow
        )
      )
  def expectedQueryPattern: String = """
  SELECT *
  FROM Products
  WHERE id IN (
      SELECT id
      FROM Purchases
  )
  """
}

class NonEmptyTest extends SQLStringQueryTest[AllCommerceDBs, Product] {
  def testDescription: String = "NonEmpty"
  def query() =
    testDB.tables.products
      .filter(p =>
        testDB.tables.purchases.filter(
          purch =>
            purch.id == p.id
        ).nonEmpty()
      )
  def expectedQueryPattern: String = """SELECT *
     FROM Products p
     WHERE EXISTS (
       SELECT 1
       FROM purchases
       WHERE purchases.id = p.id
     )"""
}

class IsEmptyTest extends SQLStringQueryTest[AllCommerceDBs, Product] {
  def testDescription: String = "Empty"
  def query() =
    testDB.tables.products
      .filter(p => testDB.tables.purchases.filter(purch => purch.id == p.id).isEmpty())
  def expectedQueryPattern: String = """SELECT *
   FROM Products p
   WHERE NOT EXISTS (
     SELECT 1
     FROM purchases
     WHERE purchases.id = p.id
   )"""
}

// class CaseTest extends SQLStringQueryTest[AllCommerceDBs, Int] {
//   def testDescription: String = "CaseTest"
//   def query() =
//     testDB.tables.products
//       .map: prod => (name: prod.name, price: prod.price, op: (prod.price > 1000) ? "Expensive" "Cheap")
//   def expectedQueryPattern: String = """
// SELECT
//   Name,
//   Price,
//   CASE
//     WHEN Price > 1000 THEN 'Expensive'
//     ELSE 'Cheap'
//   END
// FROM Products;
//   """
// }
 */