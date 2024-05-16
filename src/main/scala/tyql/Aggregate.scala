package tyql

/** TODO: Can chose to distinguish aggregation from regular select statements because they will
 * return only 1 element. So the type system could prevent further chaining of operations, given
 * that any expression with an aggregation would't return an iterable, just a value. Alternatively,
 * the library could just treat it like an iterable of length 1, so the type system should not
 * care if an operation is an aggregation or a regular expression.
 *
 * This class would represent the first option where Aggregations do not extend expression,
 * instead they are an alterantive return type from a query. The second option is implemented in
 * Expr.scala with `AggregationExpr` which does not differentiate aggregations from any other op.
 **/

trait Aggregation[A] extends DatabaseAST[A]
object Aggregation:
  import Expr.Fun
  case class Sum[A, B]($q: Query[A], $p: Fun[A, Expr[B]]) extends Aggregation[B]
  case class Avg[A, B]($q: Query[A], $p: Fun[A, Expr[B]]) extends Aggregation[B]

