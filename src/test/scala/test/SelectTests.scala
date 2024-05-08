package test
import tyql.*
import language.experimental.namedTuples
import NamedTuple.*

case class CityT(zipCode: Int, name: String, population: Int)
type AddressT = (city: CityT, street: String, number: Int)

given cityDB: TestDatabase[Tuple1[CityT]] with
  override def tables = Tuple(
    Table[CityT]("cities")
  )

given addressDB: TestDatabase[Tuple1[AddressT]] with
  override def tables = Tuple(
    Table[AddressT]("addresses")
  )

given TestDatabase[(CityT, AddressT, CityT)] with
  override def tables = (
    Table[CityT]("cities"),
    Table[AddressT]("addresses"),
    Table[CityT]("cities2")
  )

class SimpleSelectTest extends SQLStringTest[Tuple1[CityT], Int] {
  def testDescription: String = "Simple select field"
  def query() =
    testDB.tables.head.map: c =>
      c.zipCode
  def sqlString: String = "SELECT zipcode FROM cities"
}

class SelectWithFilterTest extends SQLStringTest[Tuple1[CityT], String] {
  def testDescription: String = "select with filter"
  def query() =
    testDB.tables.head.withFilter: city =>
      city.population > 10_000
    .map: city =>
      city.name
  def sqlString: String = "SELECT name FROM cities WHERE city.population > 10000"
}

class SelectWithGtTest extends SQLStringTest[Tuple1[CityT], String] {
  def testDescription: String = "select with gt constraint"
  def query() =
    for
      city <- testDB.tables.head if city.population > 10_000
    yield city.name
  def sqlString: String = "SELECT name from cities where city.population > 10000"
}

class SelectWithSelfNestTest extends SQLStringTest[Tuple1[CityT], CityT] {
  def testDescription: String = "select with nested loop, 1 table"
  def query() =
    for
      city <- testDB.tables.head
      alt <- testDB.tables.head
      if city.name == alt.name && city.zipCode != alt.zipCode
    yield
      city
  def sqlString: String = "SELECT city.* FROM cities AS city JOIN cities AS alt ON city.name=alt.name AND city.zipcode != alt.zipcode"  
}

class SelectNested extends SQLStringTest[(CityT, AddressT, CityT), CityT] {
  def testDescription: String = "select with 1 join, 2 tables, 1 row type"
  def query() =
    for
      city <- testDB.tables.head
      alt <- testDB.tables.head
      if city.name == alt.name && city.zipCode != alt.zipCode
    yield
      city
  def sqlString: String =  "SELECT city.* FROM cities AS city JOIN cities AS alt ON city.name=alt.name AND city.zipcode != alt.zipcode"
}

class SelectWithProjectTest extends SQLStringTest[Tuple1[CityT], (name: String, zipCode: Int)] {
  def testDescription: String = "select with project"
  def query() =
    val q = testDB.tables.head.map: city =>
      (name = city.name, zipCode = city.zipCode)
    q
  def sqlString: String = "SELECT city.name AS name, city.zipcode AS zipcode FROM cities AS city"
}