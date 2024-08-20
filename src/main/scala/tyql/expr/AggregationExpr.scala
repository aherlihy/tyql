package tyql

import tyql.Expr.StripExpr

import language.experimental.namedTuples
import NamedTuple.{AnyNamedTuple, NamedTuple}

import Expr.{Fun, Pred}

/**
 * A scalar operation, e.g. an expression that returns a single result, instead of a collection.
 */
trait AggregationExpr[Result](using ResultTag[Result]) extends Expr[Result, ScalarExpr]
object AggregationExpr {

  case class Sum[A: ResultTag]($a: Expr[A, ?]) extends AggregationExpr[A]

  case class Avg[A: ResultTag]($a: Expr[A, ?]) extends AggregationExpr[A]

  case class Max[A: ResultTag]($a: Expr[A, ?]) extends AggregationExpr[A]

  case class Min[A: ResultTag]($a: Expr[A, ?]) extends AggregationExpr[A]

  case class Count[A]($a: Expr[A, ?]) extends AggregationExpr[Int]

  // Needed because project can be a top-level result for aggregation but not query
  case class AggProject[A <: AnyNamedTuple]($a: A)(using ResultTag[NamedTuple.Map[A, StripExpr]]) extends AggregationExpr[NamedTuple.Map[A, StripExpr]]

//  type IsTupleOfAgg[A <: AnyNamedTuple] <: Boolean = A match {
//    case EmptyTuple => false
//    case h *: t => h match {
//      case Expr[?, ScalarExpr] => true
//      case _ => IsTupleOfAgg[t]
//    }
//  } /*(using ev: IsTupleOfAgg[A] =:= true)*/
// For now restrict all elements to be agg instead of allowing a mix.
  type IsTupleOfAgg[A <: AnyNamedTuple] = Tuple.Union[NamedTuple.DropNames[A]] <:< Expr[?, ScalarExpr]

  extension [A <: AnyNamedTuple: IsTupleOfAgg](x: A)
    def toRow(using ResultTag[NamedTuple.Map[A, StripExpr]]): AggProject[A] = AggProject(x)

  /** Same as _.toRow, as an implicit conversion */
//  given [A <: AnyNamedTuple : IsTupleOfAgg](using ResultTag[NamedTuple.Map[A, StripExpr]]): Conversion[A, AggProject[A]] = AggProject(_)

  /**
   * NOTE: For group by, require that the result is a named tuple so that it can be referred to in the next clause?
   * Also require groupBy to occur on an aggregation only
   * Otherwise have groupBy on a flat list which does not have meaning.
   */
//  extension[R/* <: AnyNamedTuple*/] (x: Aggregation[R] )
//    // For now, dont treat HAVING as anything special since AST transform will be responsible for special casing filter + groupBy
//    // groupBy will return a Query (not agg) since it will usually be an iterable
//    def groupBy[B, C](f: Expr.Ref[R] => Expr[B]): Query[R] =
//      val ref1 = Expr.Ref[R]()
//      GroupBy(x, Fun(ref1, f(ref1)))
}
