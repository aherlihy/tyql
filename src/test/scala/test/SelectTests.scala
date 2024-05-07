package test
import tyql.*

case class CityT(zipCode: Int, name: String, population: Int)
case class CityT2(zipCode: Int, name: String, population: Int)
type AddressT = (city: CityT, street: String, number: Int)
type PersonT = (name: String, age: Int, addr: AddressT)

given TestTable[CityT] with
  def testTable = Table[CityT]("cities")

class SimpleSelectTest extends SQLStringTest[CityT, Int] {
  def testDescription: String = "Simple select field"
  def query() =
    testTable.table.map: c =>
      c.zipCode
  def sqlString: String = "SELECT zipcode FROM cities"
}

class SelectWithFilterTest extends SQLStringTest[CityT, String] {
  def testDescription: String = "select with filter"
  def query() =
    testTable.table.withFilter: city =>
      city.population > 10_000
    .map: city =>
      city.name
  def sqlString: String = "SELECT name FROM cities WHERE city.population > 10000"
}

class SelectWithGtTest extends SQLStringTest[CityT, String] {
  def testDescription: String = "select with gt constraint"
  def query() =
    for
      city <- testTable.table if city.population > 10_000
    yield city.name
  def sqlString: String = "SELECT name from cities where city.population > 10000"
}

class SelectWithNestTest extends SQLStringTest[CityT, CityT] {
  def testDescription: String = "select with nested loop, 1 table"
  def query() =
    for
      city <- testTable.table
      alt <- testTable.table
      if city.name == alt.name && city.zipCode != alt.zipCode
    yield
      city
  def sqlString: String = "SELECT city.* FROM cities AS city JOIN cities AS alt ON city.name=alt.name AND city.zipcode != alt.zipcode"  
}

// class SelectWithProjectTest extends SQLStringTest[CityT, CityT] {
//   def testDescription: String = "select with project"
//   def query() =
//     testTable.table.map: city =>
//       (name = city.name, zipCode = city.zipCode)
//   def sqlString: String = "SELECT city.name AS name, city.zipcode AS zipcode FROM cities AS city"  
// }