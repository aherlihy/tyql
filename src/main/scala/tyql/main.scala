package tyql

import language.experimental.namedTuples
import scala.language.implicitConversions
import NamedTuple.{NamedTuple, AnyNamedTuple}

def run[T](q: Query[T]): Iterator[T] = ???
// //  def x1: Iterator[Int] = run(q1)
// //  def x2: Iterator[String] = run(q2)
// //  def x3: Iterator[String] = run(q3)
// //  def x4: Iterator[City] = run(q4)
// //  def x5: Iterator[(name: String, num: Int)] = run(q5)
// //  def x6: Iterator[(name: String, zipCode: Int)] = run(q6)
