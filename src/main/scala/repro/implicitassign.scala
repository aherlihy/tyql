package repro

import language.experimental.namedTuples
import scala.language.implicitConversions
import NamedTuple.{NamedTuple, AnyNamedTuple}

// Repros for bugs or questions
class Query1[A]()
object Query1:
  extension [R](x: Query1[R])
    def map[B](f: Expr1[R] => Expr1[B]): Query1[B] = ???
end Query1

trait Expr1[Result]() extends Selectable:
  type Fields = NamedTuple.Map[NamedTuple.From[Result], Expr1]
  def selectDynamic(fieldName: String) = Expr1.Select(this, fieldName)

object Expr1:
  case class Select[A]($x: Expr1[A], $name: String) extends Expr1
  case class Project[A <: AnyNamedTuple]($a: A) extends Expr1[NamedTuple.Map[A, StripExpr1]]

  type StripExpr1[E] = E match
    case Expr1[b] => b

  case class IntLit($value: Int) extends Expr1[Int]
  given Conversion[Int, IntLit] = IntLit(_)

  case class StringLit($value: String) extends Expr1[String]
  given Conversion[String, StringLit] = StringLit(_)

  /** Explicit conversion from
   *      (name_1: Expr1[T_1], ..., name_n: Expr1[T_n])
   *  to
   *      Expr1[(name_1: T_1, ..., name_n: T_n)]
   */
  extension [A <: AnyNamedTuple](x: A) def toRow: Project[A] = Project(x)
  given [A <: AnyNamedTuple]: Conversion[A, Expr1.Project[A]] = Expr1.Project(_)

end Expr1

// General test classes:
case class T1(id: Int, name: String)
trait TestQuery1[Return]:
  def t1 = Query1[T1]()
  def query(): Query1[Return]

class Repro2_Pass1 extends TestQuery1[(name: String)] {
  def query() =
    val q =
      t1.map: c =>
        (name = c.name)
    q
}
class Repro2_Pass2 extends TestQuery1[(name: String)] {
  def query() =
    t1.map: c =>
      (name = c.name).toRow
}
// Uncomment:
// class Repro2_Fail extends TestQuery1[(name: String)] {
//   override def query() =
//     t1.map: c =>
//       (name = c.name)
// }
/* Fails with:
[error] 75 |      (name = c.name)
[error]    |      ^^^^^^^^^^^^^^^
[error]    |      Found:    (name : tyql.Expr1[String])
[error]    |      Required: tyql.Expr1[(newName : String)]
*/

