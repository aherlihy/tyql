package tyql
import tyql._

trait UpdateToTheDB {
  def toSQLString(using d: Dialect)(using cnf: Config): String = toQueryIR.toSQLString()
  def toQueryIR(using d: Dialect): QueryIRNode =
    QueryIRTree.generateUpdateToTheDB(this, SymbolTable())

}

case class Insert[R](table: Table[R], names: List[String], values: Seq[Seq[?]]) extends UpdateToTheDB
case class InsertFromSelect[R, S](table: Table[R], query: Query[S, ?], names: List[String]) extends UpdateToTheDB
