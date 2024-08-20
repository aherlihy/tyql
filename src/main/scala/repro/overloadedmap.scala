//package repro
//
//import language.experimental.namedTuples
//import scala.language.implicitConversions
//import NamedTuple.{NamedTuple, AnyNamedTuple}
//
//// Repros for bugs or questions
//class Query2[A]()
//
//object Query2:
//  extension [R](x: Query2[R])
//    def map[B](f: Expr2.Ref[R, NExpr] => Expr2[B, NExpr]): Query2[B] = ???
//    def map[B <: AnyNamedTuple : Expr2.IsTupleOfExpr](f: Expr2.Ref[R, NExpr] => B): Query2[ NamedTuple.Map[B, Expr2.StripExpr2] ] = ???
//
//trait ExprShape
//class ScalarExpr extends ExprShape
//class NExpr extends ExprShape
//
//trait Expr2[Result, Shape <: ExprShape]() extends Selectable:
//  type Fields = NamedTuple.Map[NamedTuple.From[Result], [T] =>> Expr2[T, Shape]]
//
//  def selectDynamic(fieldName: String) = Expr2.Select(this, fieldName)
//
//object Expr2:
//  case class Select[A]($x: Expr2[A, ?], $name: String) extends Expr2[A, NExpr]
//
//  case class AggProject[A <: AnyNamedTuple]($a: A) extends Expr2[NamedTuple.Map[A, StripExpr2], NExpr]
//  case class Project[A <: AnyNamedTuple]($a: A) extends Expr2[NamedTuple.Map[A, StripExpr2], NExpr]
//  case class Ref[A, S<: ExprShape]() extends Expr2[A, S]
//
//
//  type StripExpr2[E] = E match
//    case Expr2[b, s] => b
//
//  type IsTupleOfExpr[A <: AnyNamedTuple] = Tuple.Union[NamedTuple.DropNames[A]] <:< Expr2[?, NExpr]
//  extension [A <: AnyNamedTuple : IsTupleOfExpr](x: A)
//    def toRow: Project[A] = ???
//
//  type IsTupleOfAgg[A <: AnyNamedTuple] = Tuple.Union[NamedTuple.DropNames[A]] <:< Expr2[?, ScalarExpr]
//  extension [A <: AnyNamedTuple : IsTupleOfAgg](x: A)
//    def toRow: AggProject[A] = ???
//
////  given [A <: AnyNamedTuple]: Conversion[A, Expr2.Project[A]] = Expr2.Project(_)
//
//// General test classes:
//case class T2(id: Int, name: String)
//
//@main def main() =
//  val t1 = Query2[T2]()
//  val q1: Query2[(newId: Int, newName: String)] = t1.map(r => (newId = r.id, newName = r.name).toRow)
//  val q2: Query2[(newId: Int, newName: String)] = t1.map(r => (newId = r.id, newName = r.name))
