package tyql

// POC of the write-to-DB AST node family.  The full implementation (Insert,
// Update, Delete, runtime lowering, dialect-specific polyfills) lives on the
// `backend-specialization` branch.

/** Base trait for AST nodes that represent a write to the database. */
trait UpdateToTheDB:
  def toSQLString(using d: dialects.Dialect): String

/** INSERT INTO <table> (<names>) <query>.  All type-safety checks for
 *  columns/values live on the `insertInto` method of [[Query]]; by the time
 *  a value of this class exists, those checks have already succeeded. */
case class InsertFromSelect[R, S](
    table: Table[R],
    query: Query[S, ?],
    names: List[String]
) extends UpdateToTheDB:
  override def toSQLString(using d: dialects.Dialect): String =
    val cols = names.map(d.quoteIdentifier).mkString(", ")
    s"INSERT INTO ${d.quoteIdentifier(table.$name)} ($cols) ${query.toSQLString}"
