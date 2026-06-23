package tyql

/**
 * Shared supertype of query and aggregation
 * @tparam Result
 */
trait DatabaseAST[Result](using val qTag: ResultTag[Result]):
  // The dialect (backend) is visible at runtime so the codegen can specialize
  // the generated SQL to it (e.g. USING KEY on DuckDB, CYCLE on PostgreSQL).
  // Defaults to the ANSI dialect when no specific one is imported.
  def toSQLString(using d: dialects.Dialect): String = toQueryIR.toSQLString()

  def toQueryIR(using d: dialects.Dialect): QueryIRNode =
    QueryIRTree.generateFullQuery(this, SymbolTable())

