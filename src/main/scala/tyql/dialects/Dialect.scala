package tyql
package dialects

/**
 * A SQL dialect: the knob that lets one TyQL query be rendered as the SQL that a
 * particular RDBMS actually accepts.
 *
 * A dialect is selected with ad-hoc polymorphism: `toSQLString` takes a
 * `(using Dialect)`, and a user picks a backend by importing its given, e.g.
 *
 * {{{
 *   import tyql.dialects.postgresql.given
 *   query.toSQLString
 * }}}
 *
 * Imported givens take precedence over the companion-object default, so a query
 * compiled with no import at all still renders as DuckDB, which is what TyQL has
 * always targeted.
 *
 * Every member here corresponds to a place where the SQL of the Recursive Query
 * Benchmark genuinely differs between engines (see the `rqb_benchmark` reference queries):
 * literal syntax, string concatenation, list values, and the shape of the
 * recursive term of a `WITH RECURSIVE` block.
 */
trait Dialect:
  def name: String

  // ---------------------------------------------------------------- literals

  /** Standard SQL string literal: single quotes, an embedded quote doubled.
   *  Double quotes -- what TyQL emitted before dialects existed -- are
   *  *identifier* syntax in DuckDB, PostgreSQL, SQLite and MariaDB alike, so
   *  there is no backend for which they are correct. */
  def stringLiteral(v: String): String = "'" + v.replace("'", "''") + "'"

  /** The `TRUE`/`FALSE` keywords, accepted by all four supported backends. */
  def booleanLiteral(v: Boolean): String = if v then "TRUE" else "FALSE"

  // ----------------------------------------------------------- concatenation

  /** How this backend concatenates strings. ANSI (and DuckDB, PostgreSQL,
   *  SQLite) spell it `||`; MariaDB reads `||` as logical OR and needs
   *  `CONCAT`. */
  def concat(parts: String*): String = parts.mkString(" || ")

  // -------------------------------------------------------------- list values

  /** A list value built from its (already rendered) elements. */
  def listLiteral(elements: Seq[String]): String

  /** Append `elem` to the end of `list`. */
  def listAppend(list: String, elem: String): String

  /** Prepend `elem` to the front of `list`. */
  def listPrepend(elem: String, list: String): String

  /** Membership test. Parenthesized, because it is most often used under a
   *  `NOT` and the polyfilled form is an infix `LIKE` rather than a call. */
  def listContains(list: String, elem: String): String

  /** Number of elements in `list`. */
  def listLength(list: String): String

  // ------------------------------------------------------------- compounds

  /**
   * How one branch of a compound query (`UNION`, `INTERSECT`, `EXCEPT`) is
   * wrapped.
   *
   * DuckDB, PostgreSQL and MariaDB all accept a parenthesized operand, and the
   * parentheses are load-bearing on DuckDB: they fix how a recursive query is
   * grouped. SQLite's grammar has no production for a parenthesized operand at
   * all -- `SELECT 1 UNION (SELECT 2)` is a syntax error -- so there every
   * branch must be bare.
   */
  def compoundOperand: SelectFlags = SelectFlags.ExprLevel

  // --------------------------------------------------------------- recursion

  /**
   * Build the recursive term of a single `WITH RECURSIVE <cteAlias> AS (base
   * UNION <term>)` block, given the term as an n-ary union of the recursive
   * arms.
   *
   * The default is the term as-is, parenthesized: grouping the arms as
   * `base UNION (arm1 UNION arm2 ...)` is not cosmetic, it fixes where the
   * engine splits the anchor from the recursive part.
   *
   * PostgreSQL overrides this to work around its "recursive reference must not
   * appear more than once" restriction.
   */
  def recursiveTerm(cteAlias: String, term: NaryRelationOp, ast: DatabaseAST[?]): QueryIRNode =
    term.appendFlag(SelectFlags.ExprLevel)

/**
 * A backend with no native list type, which encodes a list as a delimited
 * string. This is the portable encoding the RQB reference queries use for every
 * engine but DuckDB: `[1, 2]` becomes `',1,2,'`, membership is `LIKE` over the
 * delimited string, and the element count is the number of delimiters.
 */
trait DelimitedListEncoding extends Dialect:
  protected val delimiter: String = ","

  /** Hook for a backend that must pin down the width of a freshly built list
   *  value; see [[mariadb]]. */
  protected def widenListLiteral(rendered: String): String = rendered

  private def delim: String = stringLiteral(delimiter)

  override def listLiteral(elements: Seq[String]): String =
    widenListLiteral(concat((delim +: elements.flatMap(e => Seq(e, delim)))*))

  override def listAppend(list: String, elem: String): String =
    concat(list, elem, delim)

  override def listPrepend(elem: String, list: String): String =
    concat(delim, elem, list)

  override def listContains(list: String, elem: String): String =
    s"($list LIKE ${concat(stringLiteral(s"%$delimiter"), elem, stringLiteral(s"$delimiter%"))})"

  override def listLength(list: String): String =
    s"(LENGTH($list) - LENGTH(REPLACE($list, $delim, ${stringLiteral("")})))"

/**
 * DuckDB. The backend TyQL has always targeted, and the only one of the four
 * with a native list type (`[a, b]`, `list_append`, `list_contains`).
 */
object duckdb:
  given duck: Dialect = new Dialect:
    def name = "DuckDB"
    def listLiteral(elements: Seq[String]): String = elements.mkString("[", ", ", "]")
    def listAppend(list: String, elem: String): String = s"list_append($list, $elem)"
    def listPrepend(elem: String, list: String): String = s"list_prepend($elem, $list)"
    def listContains(list: String, elem: String): String = s"list_contains($list, $elem)"
    def listLength(list: String): String = s"length($list)"

/**
 * PostgreSQL. Has no list type, so it takes the delimited-string encoding, and
 * forbids referencing the recursive relation more than once in a recursive
 * term -- see [[PostgresRecursion]].
 */
object postgresql:
  given postgres: Dialect = new Dialect with DelimitedListEncoding with PostgresRecursion:
    def name = "PostgreSQL"

/**
 * SQLite. Delimited-string lists and `||` concatenation, plus the one syntactic
 * restriction none of the other three share: a branch of a compound query
 * cannot be parenthesized.
 */
object sqlite:
  given sqlite: Dialect = new Dialect with DelimitedListEncoding:
    def name = "SQLite"

    // SQLite cannot parenthesize a branch of a compound query, so neither the
    // individual branches nor the group of recursive terms may be wrapped.
    override def compoundOperand: SelectFlags = SelectFlags.Final
    override def recursiveTerm(cteAlias: String, term: NaryRelationOp, ast: DatabaseAST[?]): QueryIRNode =
      term.appendFlag(SelectFlags.Final)

/**
 * MariaDB. Reads `||` as logical OR, so strings are concatenated with `CONCAT`.
 * It also infers a recursive CTE column's width from the anchor row, and would
 * silently truncate a growing list, so a list literal is cast to a wide `CHAR`.
 */
object mariadb:
  given maria: Dialect = new Dialect with DelimitedListEncoding:
    def name = "MariaDB"
    override def concat(parts: String*): String = parts.mkString("CONCAT(", ", ", ")")
    override protected def widenListLiteral(rendered: String): String =
      s"CAST($rendered AS CHAR($listCharWidth))"

  /** Width given to a recursive CTE's list column. */
  val listCharWidth: Int = 4000

object Dialect:
  /** The default when no dialect is imported: DuckDB, which is what TyQL
   *  targeted before dialects existed. An imported given shadows this. */
  given default: Dialect = duckdb.duck
