package tyql

import language.experimental.namedTuples
import scala.language.implicitConversions
import NamedTuple.{NamedTuple, AnyNamedTuple}

// Repros for bugs or questions
class Query1[A]()
object Query1:
  import Expr1.{Pred, Fun, Ref}

  case class Filter[A]($q: Query1[A], $p: Pred[A]) extends Query1[A]
  case class Map[A, B]($q: Query1[A], $f: Fun[A, Expr1[B]]) extends Query1[B]
  extension [R](x: Query1[R])

    def withFilter(p: Ref[R] => Expr1[Boolean]): Query1[R] =
      val ref = Ref[R]()
      Filter(x, Fun(ref, p(ref)))

    def map[B](f: Ref[R] => Expr1[B]): Query1[B] =
      val ref = Ref[R]()
      Map(x, Fun(ref, f(ref)))

end Query1

trait Expr1[Result] extends Selectable:
  type Fields = NamedTuple.Map[NamedTuple.From[Result], Expr1]
  def selectDynamic(fieldName: String) = Expr1.Select(this, fieldName)
  def == (other: Expr1[?]): Expr1[Boolean] = Expr1.Eq(this, other)

object Expr1:

  extension (x: Expr1[Boolean])
    def && (y: Expr1[Boolean]): Expr1[Boolean] = And(x, y)
  case class And($x: Expr1[Boolean], $y: Expr1[Boolean]) extends Expr1[Boolean]

  case class Select[A]($x: Expr1[A], $name: String) extends Expr1
  case class Project[A <: AnyNamedTuple]($a: A) extends Expr1[NamedTuple.Map[A, StripExpr1]]

  type StripExpr1[E] = E match
    case Expr1[b] => b

  case class Eq($x: Expr1[?], $y: Expr1[?]) extends Expr1[Boolean]

  // /** References are placeholders for parameters */
  private var refCount = 0

  case class Ref[A]($name: String = "") extends Expr1[A]:
    private val id = refCount
    refCount += 1
    override def toString = s"ref$id(${$name})"

  case class IntLit($value: Int) extends Expr1[Int]
  given Conversion[Int, IntLit] = IntLit(_)

  case class StringLit($value: String) extends Expr1[String]
  given Conversion[String, StringLit] = StringLit(_)

  case class Fun[A, B]($param: Ref[A], $f: B)
  type Pred[A] = Fun[A, Expr1[Boolean]]

  /** Explicit conversion from
   *      (name_1: Expr1[T_1], ..., name_n: Expr1[T_n])
   *  to
   *      Expr1[(name_1: T_1, ..., name_n: T_n)]
   */
  extension [A <: AnyNamedTuple](x: A) def toRow: Project[A] = Project(x)
  given [A <: AnyNamedTuple]: Conversion[A, Expr1.Project[A]] = Expr1.Project(_)

end Expr1

// General test classes:
abstract class TestDatabase1[Rows <: AnyNamedTuple]: // DB takes `Rows`, e.g. named tuple of each (tableName: row type)
  def tables: NamedTuple.Map[Rows, Query1]
trait TestQuery1[Rows <: AnyNamedTuple, Return](using val testDB: TestDatabase1[Rows]): // TestQuery takes `Rows` + query return type
  def query(): Query1[Return]

// Instance data, two tables t1 and t2
case class T1(id: Int, name: String) // any old table
case class T2(id: Int, name: String)
given TDB: TestDatabase1[(t1: T1, t2: T2)] with
  override def tables = (
    t1 = Query1[T1](),
    t2 = Query1[T2]()
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
[error]    |      Found:    (name : tyql.Expr1[String])
[error]    |      Required: tyql.Expr1[(newName : String)]
*/

// REPRO3: implicit conversion doesn't happen for nested Expr1ession
class Repro3_Pass1(using TestDatabase1[TDatabase]) extends TestQuery1[TDatabase, (name: String)] {
  def query() =
    for
      t1 <- testDB.tables.t1
      if t1.name == "constant"
    yield (name = t1.name).toRow
}
class Repro3_Pass2(using TestDatabase1[TDatabase]) extends TestQuery1[TDatabase, (name: String)] {
  def query() =
    for
      t1 <- testDB.tables.t1
      if t1.name == Expr1.StringLit("constant") && t1.id == Expr1.IntLit(5)
    yield (name = t1.name).toRow
}
// Uncomment:
// class Repro3_Fail(using TestDatabase1[TDatabase]) extends TestQuery1[TDatabase, (name: String)] {
//   def query() =
//     for
//       t1 <- testDB.tables.t1
//       if t1.name == "constant" && t1.id == 5
//     yield (name = t1.name).toRow
// }
/* Fails with:
[error] -- [E172] Type Error: /Users/anna/lamp/tyql/src/main/scala/tyql/main.scala:112:9
[error] 112 |      if t1.name == "constant" && t1.id == 5
[error]     |         ^^^^^^^^^^^^^^^^^^^^^
[error]     |Values of types tyql.Expr1[String] and String cannot be compared with == or !=
[error] -- [E172] Type Error: /Users/anna/lamp/tyql/src/main/scala/tyql/main.scala:112:34
[error] 112 |      if t1.name == "constant" && t1.id == 5
[error]     |                                  ^^^^^^^^^^
[error]     |Values of types tyql.Expr1[Int] and Int cannot be compared with == or !=

>>> Workaround: add explicit `==` that takes in literal types in trait Expr1
Maybe the compiler looks for == that returns a type which has a member called &&, and when it looks like at Expr1[Boolean] it doesn't find the member && because it's an extension method?
*/

@main def main() =
  println("test")