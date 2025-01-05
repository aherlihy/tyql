package tyql
import tyql._

trait UpdateToTheDB {
  def toSQLString(using d: Dialect)(using cnf: Config): String = toQueryIR.toSQLString()
  def toQueryIR(using d: Dialect): QueryIRNode =
    QueryIRTree.generateUpdateToTheDB(this, SymbolTable())

}

case class Insert[R](table: Table[R], names: List[String], values: Seq[Seq[?]]) extends UpdateToTheDB
case class InsertFromSelect[R, S](table: Table[R], query: Query[S, ?], names: List[String]) extends UpdateToTheDB

case class Delete[R]
  (
      table: Table[R],
      p: Expr.Pred[R, NonScalarExpr],
      orderBys: Seq[(Expr.Fun[R, ?, NonScalarExpr], tyql.Ord)],
      limit: Option[Long]
  ) extends UpdateToTheDB {
  // XXX limit in DBs (e.g. MariaDB does NOT accept an expression, only literal integer)
  def limit(n: Long)(using DialectFeature.AcceptsLimitInDeletes): Delete[R] = copy(limit = Some(n))

  def orderBy[B]
    (e: Expr.Ref[R, NonScalarExpr] => Expr[B, NonScalarExpr], ord: tyql.Ord = tyql.Ord.ASC)
    (using DialectFeature.AcceptsOrderByInDeletes)
    (using ResultTag[R])
    : Delete[R] =
    val ref = Expr.Ref[R, NonScalarExpr]()
    copy(orderBys = orderBys :+ (Expr.Fun(ref, e(ref)), ord))
}
