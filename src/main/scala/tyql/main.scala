package tyql

import language.experimental.namedTuples
import scala.language.implicitConversions
import NamedTuple.{NamedTuple, AnyNamedTuple}

// Repros for bugs or questions

// General test classes:
abstract class TestDatabase1[Rows <: AnyNamedTuple]: // DB takes `Rows`, e.g. named tuple of each (tableName: row type)
  def tables: NamedTuple.Map[Rows, Table]
trait TestQuery1[Rows <: AnyNamedTuple, Return](using val testDB: TestDatabase1[Rows]): // TestQuery takes `Rows` + query return type
  def query(): Query[Return]

// Instance data, two tables t1 and t2
case class T1(id: Int, name: String) // any old table
case class T2(id: Int, name: String)
given TDB: TestDatabase1[(t1: T1, t2: T2)] with
  override def tables = (
    t1 = Table[T1]("t1"),
    t2 = Table[T2]("t2")
  )

// REPRO1: using the type alias compiles, but using the named tuple type directly fails to compile.
type TDatabase = (t1: T1, t2: T2)
class Repro1_Pass(using TestDatabase1[TDatabase]) extends TestQuery1[TDatabase, Int] {
  override def query() =
    testDB.tables.t1.map: c =>
      c.id
}
// Uncomment:
// class Repro1_Fail(using TestDatabase1[(t1: T1, t2: T2)]) extends TestQuery1[(t1: T1, t2: T2), Int] {
//   def query() =
//     testDB.tables.t1.map: c =>
//       c.id
// }
/* Fails with:
[error] 32 |    testDB.tables.t1.map: c =>
[error]    |    ^^^^^^^^^^^^^
[error]    |Found:    (x$proxy3 :
[error]    |  (Repro1_Fail.this.testDB.tables :
[error]    |    => NamedTuple.Map[(t1 : tyql.T1, t2 : tyql.T2), tyql.Table])
[error]    | &
[error]    |  $proxy3.NamedTuple[
[error]    |    NamedTupleDecomposition.Names[
[error]    |      $proxy3.NamedTuple[(("t1" : String), ("t2" : String)), (tyql.T1, tyql.T2)]
[error]    |      ],
[error]    |    Tuple.Map²[
[error]    |      NamedTupleDecomposition.DropNames[
[error]    |        $proxy3.NamedTuple[(("t1" : String), ("t2" : String)), (tyql.T1, tyql.T2
[error]    |          )]
[error]    |      ],
[error]    |    tyql.Table]
[error]    |  ]
[error]    |)
[error]    |Required: (tyql.Table[tyql.T1], tyql.Table[tyql.T2])
 */

// REPRO2: implicit conversion only works with temporary variable (intentional?)
class Repro2_Pass1(using TestDatabase1[TDatabase]) extends TestQuery1[TDatabase, (name: String)] {
  def query() =
    val q =
      testDB.tables.t1.map: c =>
        (name = c.name)
    q
}
class Repro2_Pass2(using TestDatabase1[TDatabase]) extends TestQuery1[TDatabase, (name: String)] {
  def query() =
    testDB.tables.t1.map: c =>
      (name = c.name).toRow
}
// Uncomment:
// class Repro2_Fail(using TestDatabase1[TDatabase]) extends TestQuery1[TDatabase, (name: String)] {
//   override def query() =
//     testDB.tables.t1.map: c =>
//       (name = c.name)
// }
/* Fails with:
[error] 75 |      (name = c.name)
[error]    |      ^^^^^^^^^^^^^^^
[error]    |      Found:    (name : tyql.Expr[String])
[error]    |      Required: tyql.Expr[(newName : String)]
*/


@main def main() =
  println("test")