package tyql

/** TODO: Can chose to distinguish aggregation from regular select statements because they will
 * return only 1 element. So the type system could prevent further chaining of operations, given
 * that any expression with an aggregation would't return an iterable, just a value. Alternatively,
 * the library could just treat it like an iterable of length 1, so the type system should not
 * care if an operation is an aggregation or a regular expression.
 *
 * Currently support ASTs of Map[Aggregation] as well as Aggregation[Aggregation] depending on user code
 *
 **/
import Expr.{Fun, AggregationExpr}

case class Aggregation[
  QueryType,
  InputToAgg,
  AggregateReturnType // For many will be the same as input type (e.g. sum) but not always
]($this: Query[QueryType], $f: Fun[QueryType, AggregationExpr[InputToAgg]]) extends Query[AggregateReturnType]
