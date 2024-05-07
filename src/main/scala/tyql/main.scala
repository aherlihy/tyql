package tyql

import language.experimental.namedTuples
import scala.language.implicitConversions
import NamedTuple.{NamedTuple, AnyNamedTuple}

// Some sample types
case class City(zipCode: Int, name: String, population: Int)
case class City2(zipCode: Int, name: String, population: Int)
type Address = (city: City, street: String, number: Int)
type Person = (name: String, age: Int, addr: Address)

// case class QueryTest[Row, Result](name: String, // TODO: constrain Result based on Rows? Constrain types to DB types?
//                           queryFn: Table[Row] => Query[Result],
//                           sqlstr: String)
// case class QueryTest2[Row1, Row2, Result](name: String, // Unlimited # rows?
//                           queryFn: (Table[Row1], Table[Row2]) => Query[Result],
//                           sqlstr: String)                          
// val selectTests = Seq(
//   QueryTest(
//     "simple select",
//     (cities: Table[City]) =>
//       cities.map: c =>
//         c.zipCode,
//     "SELECT zipcode FROM cities"
//   ),
//   QueryTest(
//     "select with filter",
//     (cities: Table[City]) =>
//       cities.withFilter: city =>
//         city.population > 10_000
//       .map: city =>
//         city.name,
//     "SELECT name FROM cities WHERE city.population > 10000"
//   ),
//   QueryTest(
//     "select with gt constraint",
//     (cities: Table[City]) =>
//       for
//         city <- cities if city.population > 10_000
//       yield city.name,
//     "SELECT name from cities where city.population > 10000"
//   ),
//   QueryTest(
//     "select with nested loop, 1 table",
//     (cities: Table[City]) =>
//       for
//         city <- cities
//         alt <- cities
//         if city.name == alt.name && city.zipCode != alt.zipCode
//       yield
//         city,
//     "SELECT city.* FROM cities AS city JOIN cities AS alt ON city.name=alt.name AND city.zipcode != alt.zipcode"  
//   ),
//     QueryTest(
//     "select with project",
//     (cities: Table[City]) =>
//       cities.map: city =>
//         (name = city.name, zipCode = city.zipCode),
//     "SELECT city.name AS name, city.zipcode AS zipcode FROM cities AS city"  
//   )
// )

// @main def main =

//   val cities = Table[City]("cities")
//   val cities2 = Table[City]("cities2")
//   val addresses = Table[Address]("addresses")

//   def run[T](q: Query[T]): Iterator[T] = ???
// //  def x1: Iterator[Int] = run(q1)
// //  def x2: Iterator[String] = run(q2)
// //  def x3: Iterator[String] = run(q3)
// //  def x4: Iterator[City] = run(q4)
// //  def x5: Iterator[(name: String, num: Int)] = run(q5)
// //  def x6: Iterator[(name: String, zipCode: Int)] = run(q6)

//   selectTests.foreach(t =>
//     println(s"${t.name}:\n\t${t.queryFn(cities)}\n\t${t.sqlstr}")
//   )

//   val q2 = QueryTest2(
//     "select with 1 join, 2 tables, 1 row type",
//     (cities: Table[City], cities2: Table[City]) =>
//       for
//         city <- cities
//         alt <- cities2
//         if city.name == alt.name && city.zipCode != alt.zipCode
//       yield
//         city,
//       "SELECT city.* FROM cities AS city JOIN cities AS alt ON city.name=alt.name AND city.zipcode != alt.zipcode"  
//   )
//   val q3 = QueryTest2(
//     "select with 1 join, 2 tables, 2 row types",
//     (cities: Table[City], addresses: Table[Address]) =>
//       for
//         city <- cities
//         addr <- addresses
//         if addr.street == city.name
//       yield
//         (name = city.name, num = addr.number),
//         "SELECT city.name AS name, addr.number AS num FROM cities AS city JOIN addresses AS addr ON addr.street = city.name"
//   )

//   println(s"${q2.name}:\n\t$q2.queryFn(cities, cities2)}\n\t${q2.sqlstr}")
//   println(s"${q3.name}:\n\t${q3.queryFn(cities, addresses)}\n\t${q3.sqlstr}")
