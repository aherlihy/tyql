package tyql

import tyql.dialects.Dialect

/**
 * Shared supertype of query and aggregation
 * @tparam Result
 */
trait DatabaseAST[Result](using val qTag: ResultTag[Result]):
  /**
   * Render this query as SQL for the dialect in scope. With no dialect imported
   * this is DuckDB; import e.g. `tyql.dialects.postgresql.given` to target
   * another backend.
   */
  def toSQLString(using Dialect): String = toQueryIR.toSQLString()

  def toQueryIR(using Dialect): QueryIRNode =
    QueryIRTree.generateFullQuery(this, SymbolTable())
