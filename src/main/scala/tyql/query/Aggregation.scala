package tyql

trait Aggregation[Result](using ResultTag[Result]) extends DatabaseAST[Result] with Expr[Result, NonScalarExpr]
object Aggregation:
  case class AggFlatMap[A, B: ResultTag]($from: Query[A, ?], $query: Expr.Fun[A, Expr[B, ?], ?]) extends Aggregation[B]
  case class AggFilter[A: ResultTag]($from: Query[A, ?], $pred: Expr.Pred[A, ScalarExpr]) extends Aggregation[A]
