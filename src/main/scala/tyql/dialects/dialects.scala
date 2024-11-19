package tyql

import tyql.DialectFeature
import tyql.DialectFeature.*

// TODO which of these should be sealed? Do we support custom dialectes?

trait Dialect:
  def name(): String

  protected val reservedKeywords: Set[String]
  def quoteIdentifier(id: String): String

  def limitAndOffset(limit: Long, offset: Long): String

  def quoteStringLiteral(in: String, insideLikePattern: Boolean): String
  def quoteBooleanLiteral(in: Boolean): String

object Dialect:
  val literal_percent = '\uE000'
  val literal_underscore = '\uE001'

  given Dialect = new Dialect
      with QuotingIdentifiers.AnsiBehavior
      with LimitAndOffset.Separate
      with StringLiteral.AnsiSingleQuote
      with BooleanLiterals.UseTrueFalse:
    def name() = "ANSI SQL Dialect"

  object ansi:
    given Dialect = Dialect.given_Dialect

  object postgresql:
    given Dialect = new Dialect
        with QuotingIdentifiers.PostgresqlBehavior
        with LimitAndOffset.Separate
        with StringLiteral.PostgresqlBehavior
        with BooleanLiterals.UseTrueFalse:
      def name() = "PostgreSQL Dialect"
    given RandomFloat = new RandomFloat(Some("random")) {}

  object mysql:
    given Dialect = new MySQLDialect
    class MySQLDialect extends Dialect
        with QuotingIdentifiers.MysqlBehavior
        with LimitAndOffset.MysqlLike
        with StringLiteral.MysqlBehavior
        with BooleanLiterals.UseTrueFalse:
      def name() = "MySQL Dialect"

    given RandomFloat = new RandomFloat(Some("rand")) {}

  object mariadb:
    // XXX MariaDB extends MySQL
    // XXX but you still have to redeclare the givens
    given Dialect = new mysql.MySQLDialect with QuotingIdentifiers.MariadbBehavior:
      override def name() = "MariaDB Dialect"

    given RandomFloat = mysql.given_RandomFloat

  object sqlite:
    given Dialect = new Dialect
        with QuotingIdentifiers.SqliteBehavior
        with LimitAndOffset.Separate
        with StringLiteral.AnsiSingleQuote
        with BooleanLiterals.UseTrueFalse:
      def name() = "SQLite Dialect"

    given RandomFloat = new RandomFloat(None, Some("(0.5 - RANDOM() / CAST(-9223372036854775808 AS REAL) / 2)")) {}

  object h2:
    given Dialect = new Dialect
        with QuotingIdentifiers.H2Behavior
        with LimitAndOffset.Separate
        with StringLiteral.AnsiSingleQuote
        with BooleanLiterals.UseTrueFalse:
      def name() = "H2 Dialect"

    given RandomFloat = new RandomFloat(Some("rand")) {}

  object duckdb:
    given Dialect = new Dialect
        with QuotingIdentifiers.DuckdbBehavior
        with LimitAndOffset.Separate
        with StringLiteral.DuckdbBehavior
        with BooleanLiterals.UseTrueFalse:
      override def name(): String = "DuckDB Dialect"

    given RandomFloat = new RandomFloat(Some("random")) {}
