package tyql

import language.experimental.namedTuples
import NamedTuple.{AnyNamedTuple, NamedTuple}

import Expr.{Fun, Pred, StripExpr}

/** A scalar operation, e.g. an expression that returns a single result, instead of a collection.
  */
sealed trait AggregationExpr[Result](using ResultTag[Result]) extends Expr[Result, ScalarExpr] {
  def partitionBy(e: Expr[?, NonScalarExpr]*): WindowExpression[Result] = WindExpr(Left(this), e.toList, Seq())
  def orderBy(e: Expr[?, NonScalarExpr], ord: tyql.Ord): WindowExpression[Result] =
    WindExpr(Left(this), Seq(), Seq((e, ord)))
}

object AggregationExpr {

  case class Sum[A: ResultTag]($a: Expr[A, ?]) extends AggregationExpr[A]

  case class Avg[A: ResultTag]($a: Expr[A, ?]) extends AggregationExpr[A]

  case class Max[A: ResultTag]($a: Expr[A, ?]) extends AggregationExpr[A]

  case class Min[A: ResultTag]($a: Expr[A, ?]) extends AggregationExpr[A]

  case class Count[A]($a: Expr[A, ?]) extends AggregationExpr[Int]

  // Needed because project can be a top-level result for aggregation but not query
  case class AggProject[A <: AnyNamedTuple]($a: A)(using ResultTag[NamedTuple.Map[A, StripExpr]])
      extends AggregationExpr[NamedTuple.Map[A, StripExpr]]

  // For now restrict all elements to be agg instead of allowing a mix.
  type IsTupleOfAgg[A <: AnyNamedTuple] = Tuple.Union[NamedTuple.DropNames[A]] <:< (Expr[?, ScalarExpr] | LiteralExpressionsAlsoAllowedInAggregations)

  extension [A <: AnyNamedTuple : IsTupleOfAgg](x: A)
    def toRow(using ResultTag[NamedTuple.Map[A, StripExpr]]): AggProject[A] = AggProject(x)

  type GetExprShape[A] = A match
    case Expr[t, s]         => s
    case AggregationExpr[t] => ScalarExpr

  // Allow mixed aggregates and expressions for grouping
  extension [A <: AnyNamedTuple, B <: Tuple](using B <:< Tuple.Map[NamedTuple.DropNames[A], GetExprShape])(x: A)
    def toGroupingRow(using ResultTag[NamedTuple.Map[A, StripExpr]]): AggProject[A] = AggProject(x)

  /** Same as _.toRow, as an implicit conversion */
  // given [A <: AnyNamedTuple : IsTupleOfAgg](using ResultTag[NamedTuple.Map[A, StripExpr]]): Conversion[A, AggProject[A]] = AggProject(_)
}
