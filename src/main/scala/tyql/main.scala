package tyql

import language.experimental.namedTuples
import scala.language.implicitConversions
import NamedTuple.{NamedTuple, AnyNamedTuple}




//   def run[T](q: Query[T]): Iterator[T] = ???
// //  def x1: Iterator[Int] = run(q1)
// //  def x2: Iterator[String] = run(q2)
// //  def x3: Iterator[String] = run(q3)
// //  def x4: Iterator[City] = run(q4)
// //  def x5: Iterator[(name: String, num: Int)] = run(q5)
// //  def x6: Iterator[(name: String, zipCode: Int)] = run(q6)

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
