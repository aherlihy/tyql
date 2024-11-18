package tyql

/**
 * Shared supertype of query and aggregation
 * @tparam Result
 */
trait DatabaseAST[Result](using val qTag: ResultTag[Result]):
  def toSQLString(using d: Dialect)(using cnf: Config): String = toQueryIR.toSQLString()

  def toQueryIR: QueryIRNode =
    QueryIRTree.generateFullQuery(this, SymbolTable())
