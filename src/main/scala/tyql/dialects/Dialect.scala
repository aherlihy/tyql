package tyql
package dialects

// POC of the Dialect machinery.  The full implementation lives on the
// `backend-specialization` branch.

/** Base trait for SQL dialects.  Each overridable member influences at least
 *  one piece of generated SQL. */
trait Dialect:
  def name(): String

  /** How identifiers are quoted in generated SQL.  Default: ANSI double-quote. */
  def quoteIdentifier(id: String): String = "\"" + id + "\""

  /** How a string literal is rendered.  Standard SQL uses single quotes, with an
   *  embedded single quote escaped by doubling it (`''`).  Double quotes — what a
   *  naive renderer emits — are *identifier* syntax in PostgreSQL/DuckDB and would
   *  be misread as a column reference, so single-quoting is the correct portable
   *  default.  Overridable per dialect for backends with different escaping rules. */
  def quoteStringLiteral(s: String): String = "'" + s.replace("'", "''") + "'"

  /** How a boolean literal is rendered.  Standard SQL uses the TRUE/FALSE keywords
   *  (accepted by PostgreSQL, DuckDB, and MySQL). */
  def booleanLiteral(b: Boolean): String = if b then "TRUE" else "FALSE"

  /** Dialect-specific cast keyword for integer columns. */
  val integerCast: String = "INTEGER"

  /** Dialect-specific cast keyword for string columns. */
  val stringCast: String = "VARCHAR"

  /** Whether this backend supports keyed recursion (`WITH RECURSIVE ... USING KEY`),
   *  which is what lets a NON-LINEAR recursive query run correctly. Drives runtime
   *  SQL generation; the matching compile-time gate is `DialectFeature.KeyedRecursion`. */
  def supportsKeyedRecursion: Boolean = false

  /** Whether this backend supports a `CYCLE` clause to bound a bag-semantic
   *  recursive query. Compile-time gate: `DialectFeature.CycleClause`. */
  def supportsCycleClause: Boolean = false

object Dialect:
  /** Default ANSI dialect — in scope automatically whenever any Dialect is
   *  summoned and no more specific given is in scope.  Deliberately does NOT
   *  provide any DialectFeature givens, so methods that require a capability
   *  (e.g. `insertInto` via `DialectFeature.Insertable`) fail to compile until
   *  the user imports a dialect that provides them. */
  given ansi: Dialect = new Dialect:
    def name() = "ANSI SQL Dialect"

/** PostgreSQL dialect.  Users activate it with
 *  `import tyql.dialects.postgresql.given`.  When in scope it shadows the
 *  ANSI default and also provides the `Insertable` feature marker (see
 *  [[tyql.dialects.DialectFeature]]) that gates `insertInto`. */
object postgresql:
  given postgres: Dialect = new Dialect:
    def name() = "PostgreSQL Dialect"
    // Postgres uses ANSI double-quoted identifiers (inherited from the base
    // Dialect.quoteIdentifier default).
    override val integerCast: String = "BIGINT"
    override val stringCast: String = "TEXT"
    override def supportsCycleClause: Boolean = true

  /** PostgreSQL supports `INSERT ... SELECT`. */
  given DialectFeature.Insertable = new DialectFeature.Insertable {}

  /** PostgreSQL (v14+) provides a `CYCLE` clause to bound bag-semantic recursion,
   *  but supports neither keyed recursion nor aggregation inside recursion. */
  given DialectFeature.CycleClause = new DialectFeature.CycleClause {}

/** MySQL dialect.  Users activate it with `import tyql.dialects.mysql.given`.
 *  Included here to demonstrate that the *same* TyQL query source compiles
 *  to different SQL strings under different backends — MySQL quotes
 *  identifiers with backticks (`` ` ``) rather than double quotes, and uses
 *  different cast keywords. */
object mysql:
  given my: Dialect = new Dialect:
    def name() = "MySQL Dialect"
    override def quoteIdentifier(id: String): String = s"`$id`"
    override val integerCast: String = "SIGNED"
    override val stringCast: String = "CHAR"

  given DialectFeature.Insertable = new DialectFeature.Insertable {}

/** DuckDB dialect.  Users activate it with `import tyql.dialects.duckdb.given`.
 *  DuckDB uses ANSI double-quoted identifiers (inherited default).  It provides
 *  two recursive capabilities the standard does not: keyed recursion
 *  (`USING KEY`), which lets a non-linear recursive query run correctly, and
 *  aggregation inside the recursive step.  It does NOT provide a `CYCLE` clause. */
object duckdb:
  given duck: Dialect = new Dialect:
    def name() = "DuckDB Dialect"
    override def supportsKeyedRecursion: Boolean = true

  given DialectFeature.KeyedRecursion = new DialectFeature.KeyedRecursion {}
  given DialectFeature.RecursiveAggregation = new DialectFeature.RecursiveAggregation {}
