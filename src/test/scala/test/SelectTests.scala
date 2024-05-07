package test
import tyql.*

case class CityT(zipCode: Int, name: String, population: Int)
case class CityT2(zipCode: Int, name: String, population: Int)
type AddressT = (city: City, street: String, number: Int)
type PersonT = (name: String, age: Int, addr: Address)

class SimpleSelectTest extends SQLStringTest[CityT, Int] {
  def testDescription: String = "Simple select field"
  def query(): Query[Int] =
    table.map: c =>
      c.zipCode
  def sqlString: String = "SELECT zipcode FROM cities"
}