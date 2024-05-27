package tyql

import language.experimental.namedTuples
import NamedTuple.{NamedTuple, AnyNamedTuple}

/** TODO: Can chose to distinguish aggregation from regular select statements because they will
 * return only 1 element. So the type system could prevent further chaining of operations, given
 * that any expression with an aggregation wouldn't return an iterable, just a value. Could also
 * embed aggregations within queries and treat them as expressions.
 *
 * Alternatively, could just treat it like an iterable of length 1, so the type system should not
 * care if an operation is an aggregation or a regular expression.
 *
 * Currently support ASTs of Map[Aggregation] as well as Aggregation[Aggregation] depending on user code
 *
 **/
import Expr.Fun

trait Aggregation[Result] extends Expr[Result] with DatabaseAST[Result]
object Aggregation {
  case class AggFlatMap[A, B]($q: Query[A], $f: Fun[A, Aggregation[B]]) extends Aggregation[B]

  case class Sum[A]($a: Expr[A]) extends Aggregation[A]

  case class Avg[A]($a: Expr[A]) extends Aggregation[A]

  case class Max[A]($a: Expr[A]) extends Aggregation[A]

  case class Min[A]($a: Expr[A]) extends Aggregation[A]

  case class Count[A]($a: Expr[A]) extends Aggregation[Int]

  // Needed because project can be a final result for aggregation but not query
  case class AggProject[A <: AnyNamedTuple]($a: A) extends Aggregation[NamedTuple.Map[A, StripAgg]]

  type StripAgg[E] = E match
    case Aggregation[b] => b

  type IsTupleOfAgg[A <: AnyNamedTuple] = Tuple.Union[NamedTuple.DropNames[A]] <:< Aggregation[?]

  extension [A <: AnyNamedTuple : IsTupleOfAgg](x: A)
    def toRow: AggProject[A] = AggProject(x)

  /** Same as _.toRow, as an implicit conversion */
  given [A <: AnyNamedTuple : IsTupleOfAgg]: Conversion[A, Aggregation.AggProject[A]] = Aggregation.AggProject(_)

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
