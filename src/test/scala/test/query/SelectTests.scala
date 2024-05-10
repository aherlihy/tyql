package test.query.select
import test.{SQLStringTest, TestDatabase}

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

class SimpleSelectTest extends SQLStringTest[CityDB, Int] {
  def testDescription: String = "Select: Simple select field"
  def query() =
    testDB.tables.cities.map: c =>
      c.zipCode
  def sqlString: String = "SELECT zipcode FROM cities"
}

class SelectWithFilterTest extends SQLStringTest[CityDB, String] {
  def testDescription: String = "Select: select with > filter"
  def query() =
    testDB.tables.cities.withFilter: city =>
      city.population > 10_000
    .map: city =>
      city.name
  def sqlString: String = "SELECT name FROM cities WHERE city.population > 10000"
}

class SelectWithGtTest extends SQLStringTest[CityDB, String] {
  def testDescription: String = "Select: select with gt constraint"
  def query() =
    for
      city <- testDB.tables.cities if city.population > 10_000
    yield city.name
  def sqlString: String = "SELECT name from cities where city.population > 10000"
}

class SelectWithSelfNestTest extends SQLStringTest[CityDB, CityT] {
  def testDescription: String = "Select: self-join with condition"
  def query() =
    for
      city <- testDB.tables.cities
      alt <- testDB.tables.cities
      if city.name == alt.name && city.zipCode != alt.zipCode
    yield
      city
  def sqlString: String = "SELECT city.* FROM cities AS city JOIN cities AS alt ON city.name=alt.name AND city.zipcode != alt.zipcode"
}

class SelectNested extends SQLStringTest[AllLocDBs, CityT] {
  def testDescription: String = "Select: two-table join with condition "
  def query() =
    for
      city <- testDB.tables.cities
      address <- testDB.tables.addresses
      if city == address.city
    yield
      city
  def sqlString: String =  "SELECT city.* FROM cities AS city JOIN addresses AS address ON city=address.city"
}

class SelectWithProjectTestImplicit extends SQLStringTest[CityDB, (name: String, zipCode: Int)] {
  def testDescription: String = "Select: select with project"
  def query() =
    val q = testDB.tables.cities.map: city =>
      (name = city.name, zipCode = city.zipCode)
    q
  def sqlString: String = "SELECT city.name AS name, city.zipcode AS zipcode FROM cities AS city"
}

class SelectWithProjectTestToRow extends SQLStringTest[CityDB, (name: String, zipCode: Int)] {
  def testDescription: String = "Select: select with project"
  def query() =
    testDB.tables.cities.map: city =>
      (name = city.name, zipCode = city.zipCode).toRow
  def sqlString: String = "SELECT city.name AS name, city.zipcode AS zipcode FROM cities AS city"
}