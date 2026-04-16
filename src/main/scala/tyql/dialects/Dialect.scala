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

  /** Dialect-specific cast keyword for integer columns. */
  val integerCast: String = "INTEGER"

  /** Dialect-specific cast keyword for string columns. */
  val stringCast: String = "VARCHAR"

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

  /** PostgreSQL supports `INSERT ... SELECT`. */
  given DialectFeature.Insertable = new DialectFeature.Insertable {}

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
