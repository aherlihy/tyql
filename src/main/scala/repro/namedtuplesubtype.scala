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
//  val t1 = Expr2.Instance((name = "test", id = 10))
//  val t2 = Expr2.Instance((age = 100))
//  t1.concat(t2)
//  t1.toRow
  val x1 = Expr2.Instance((name = "test", id = 10).toRow)
  val x2 = Expr2.Instance((age = 100).toRow)
//  x1.toRow
//  x1.concat(x2)