package repro

import language.experimental.namedTuples
import NamedTuple.{NamedTuple, AnyNamedTuple}

trait Expr2[Result]

object Expr2:
  type StripExpr[E] = E match
    case Expr2[b] => b

  case class Instance[A](x: Expr2[A]) extends Expr2[A]
  case class Project[A <: AnyNamedTuple]($a: A) extends Expr2[NamedTuple.Map[A, StripExpr]]

  extension [A <: AnyNamedTuple](x: A)
    def toRow: Project[A] = Project(x)
    def concat[B <: AnyNamedTuple](other: Expr2[B]) = ???

//  given [A <: AnyNamedTuple]: Conversion[A, Expr2.Project[A]] = Expr2.Project(_)

object Repro3:
  import Expr2.toRow
// without implicit conversion:
  val x1 = Expr2.Instance((name = "test", id = 10).toRow)
  val x2 = Expr2.Instance((age = 100).toRow)
// Uncomment:
//  x1.toRow

/* Errors with:
[error] 32 |  x1.toRow
[error]    |  ^^^^^^^^
[error]    |value toRow is not a member of repro.Expr2.Instance[(name : repro.Expr2.StripExpr[String], id :
[error]    |  repro.Expr2.StripExpr[Int])].
[error]    |An extension method was tried, but could not be fully constructed:
[error]    |
[error]    |    repro.Expr2.toRow[A](x1)
*/

  x1.concat(x2)
/* Similar error:
[error] 42 |  x1.concat(x2)
[error]    |  ^^^^^^^^^
[error]    |value concat is not a member of repro.Expr2.Instance[(name : repro.Expr2.StripExpr[String], id :
[error]    |  repro.Expr2.StripExpr[Int])].
[error]    |An extension method was tried, but could not be fully constructed:
[error]    |
[error]    |    repro.Expr2.concat[A](x1)
*/

// with the implicit conversion (e.g. how I would like it to be used), similar error:
//  val t1 = Expr2.Instance((name = "test", id = 10))
//  val t2 = Expr2.Instance((age = 100))
//  t1.concat(t2) //  or, t1.toRow
/* Errors with:
[error] 26 |  t1.concat(t2)
[error]    |  ^^^^^^^^^
[error]    |value concat is not a member of repro.Expr2.Instance[(name : repro.Expr2.StripExpr[String], id :
[error]    |  repro.Expr2.StripExpr[Int])].
[error]    |An extension method was tried, but could not be fully constructed:
[error]    |
[error]    |    repro.Expr2.concat[A](t1)
*/