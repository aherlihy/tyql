package tyql

trait WindowExpression[Result](using ResultTag[Result]) extends Expr[Result, NonScalarExpr]

case class WindExpr[R](ae: Either[AggregationExpr[R], ExprInWindowPosition[R]], partitionBy: Seq[Expr[?, NonScalarExpr]], orderBy: Seq[(Expr[?, NonScalarExpr], tyql.Ord)])(using ResultTag[R]) extends WindowExpression[R] {
  def partitionBy(e: Expr[?, NonScalarExpr]*): WindowExpression[R] = WindExpr(ae, partitionBy ++ e, Seq())
  def orderBy(e: Expr[?, NonScalarExpr], ord: tyql.Ord): WindowExpression[R] = WindExpr(ae, partitionBy, orderBy :+ (e, ord))
}

sealed trait ExprInWindowPosition[R](using ResultTag[R]) {
  def partitionBy(e: Expr[?, NonScalarExpr]*): WindowExpression[R] = WindExpr(Right(this), e.toList, Seq())
  def orderBy(e: Expr[?, NonScalarExpr], ord: tyql.Ord): WindowExpression[R] = WindExpr(Right(this), Seq(), Seq((e, ord)))
}

case class RowNumber() extends ExprInWindowPosition[Int]
case class Rank() extends ExprInWindowPosition[Int]
case class DenseRank() extends ExprInWindowPosition[Int]
case class NTile(n: Int) extends ExprInWindowPosition[Int]
case class Lag[R, S <: ExprShape](e: Expr[R, S], offset: Option[Int], default: Option[Expr[R, S]])(using ResultTag[R]) extends ExprInWindowPosition[R]
case class Lead[R, S <: ExprShape](e: Expr[R, S], offset: Option[Int], default: Option[Expr[R, S]])(using ResultTag[R]) extends ExprInWindowPosition[R]
case class FirstValue[R, S <: ExprShape](e: Expr[R, S])(using ResultTag[R]) extends ExprInWindowPosition[R]
case class LastValue[R, S <: ExprShape](e: Expr[R, S])(using ResultTag[R]) extends ExprInWindowPosition[R]
case class NthValue[R, S <: ExprShape](e: Expr[R, S], n: Int)(using ResultTag[R]) extends ExprInWindowPosition[R]
