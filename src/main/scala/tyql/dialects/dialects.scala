package tyql

import tyql.DialectFeature
import tyql.DialectFeature.*

// TODO which of these should be sealed? Do we support custom dialectes?

private def unsupportedFeature(feature: String) =
  throw new UnsupportedOperationException(s"$feature feature not supported in this dialect!")

trait Dialect:
  def name(): String

  protected val reservedKeywords: Set[String]
  def quoteIdentifier(id: String): String

  def limitAndOffset(limit: Long, offset: Long): String

  def quoteStringLiteral(in: String, insideLikePattern: Boolean): String
  def quoteBooleanLiteral(in: Boolean): String

  val stringLengthByCharacters: String = "CHAR_LENGTH"
  val stringLengthByBytes: Seq[String] = Seq("OCTET_LENGTH") // series of functions to nest, in order from inner to outer

  val xorOperatorSupportedNatively = false

  def feature_RandomUUID_functionName: String = unsupportedFeature("RandomUUID")
  def feature_RandomFloat_functionName: Option[String] = unsupportedFeature("RandomFloat")
  def feature_RandomFloat_rawSQL: Option[String] = unsupportedFeature("RandomFloat")
  def feature_RandomInt_rawSQL(a: String, b: String): String = unsupportedFeature("RandomInt")

  def needsStringRepeatPolyfill: Boolean = false
  def needsStringLPadRPadPolyfill: Boolean = false

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
      override val stringLengthByCharacters: String = "length"
      override def feature_RandomUUID_functionName: String = "gen_random_uuid"
      override def feature_RandomFloat_functionName: Option[String] = Some("random")
      override def feature_RandomFloat_rawSQL: Option[String] = None
      override def feature_RandomInt_rawSQL(a: String, b: String): String = s"floor(random() * ($b - $a + 1) + $a)::integer"

    given RandomFloat = new RandomFloat {}
    given RandomUUID = new RandomUUID {}
    // TODO now that we have precedence, fix the parenthesization rules for this!
    given RandomIntegerInInclusiveRange = new RandomIntegerInInclusiveRange {}
    given ReversibleStrings = new ReversibleStrings {}

  object mysql:
    given Dialect = new MySQLDialect
    class MySQLDialect extends Dialect
        with QuotingIdentifiers.MysqlBehavior
        with LimitAndOffset.MysqlLike
        with StringLiteral.MysqlBehavior
        with BooleanLiterals.UseTrueFalse:
      def name() = "MySQL Dialect"
      override val xorOperatorSupportedNatively = true
      override def feature_RandomUUID_functionName: String = "UUID"
      override def feature_RandomFloat_functionName: Option[String] = Some("rand")
      override def feature_RandomFloat_rawSQL: Option[String] = None
      override def feature_RandomInt_rawSQL(a: String, b: String): String = s"floor(rand() * ($b - $a + 1) + $a)"

    given RandomFloat = new RandomFloat {}
    given RandomUUID = new RandomUUID {}
    // TODO now that we have precedence, fix the parenthesization rules for this!
    given RandomIntegerInInclusiveRange = new RandomIntegerInInclusiveRange {}
    given ReversibleStrings = new ReversibleStrings {}

  object mariadb:
    // XXX MariaDB extends MySQL
    // XXX but you still have to redeclare the givens
    given Dialect = new mysql.MySQLDialect with QuotingIdentifiers.MariadbBehavior:
      override def name() = "MariaDB Dialect"

    given RandomFloat = mysql.given_RandomFloat
    given RandomUUID = mysql.given_RandomUUID
    given RandomIntegerInInclusiveRange = mysql.given_RandomIntegerInInclusiveRange
    given ReversibleStrings = mysql.given_ReversibleStrings

  object sqlite:
    given Dialect = new Dialect
        with QuotingIdentifiers.SqliteBehavior
        with LimitAndOffset.Separate
        with StringLiteral.AnsiSingleQuote
        with BooleanLiterals.UseTrueFalse:
      def name() = "SQLite Dialect"
      override val stringLengthByCharacters = "length"
      override def feature_RandomFloat_functionName: Option[String] = None
      // TODO now that we have precedence, fix the parenthesization rules for this!
      override def feature_RandomFloat_rawSQL: Option[String] = Some("(0.5 - RANDOM() / CAST(-9223372036854775808 AS REAL) / 2)")
      override def feature_RandomInt_rawSQL(a: String, b: String): String = s"cast(abs(random() % ($b - $a + 1) + $a) as integer)"
      override def needsStringRepeatPolyfill: Boolean = true
      override def needsStringLPadRPadPolyfill: Boolean = true

    // TODO think about how quoting strings like this impacts simplifications and efficient generation
    given RandomFloat = new RandomFloat {}
    // TODO now that we have precedence, fix the parenthesization rules for this!
    given RandomIntegerInInclusiveRange = new RandomIntegerInInclusiveRange {}
    given ReversibleStrings = new ReversibleStrings {}

  object h2:
    given Dialect = new Dialect
        with QuotingIdentifiers.H2Behavior
        with LimitAndOffset.Separate
        with StringLiteral.AnsiSingleQuote
        with BooleanLiterals.UseTrueFalse:
      def name() = "H2 Dialect"
      override val stringLengthByCharacters = "length"
      override def feature_RandomUUID_functionName: String = "RANDOM_UUID"
      override def feature_RandomFloat_functionName: Option[String] = Some("rand")
      override def feature_RandomFloat_rawSQL: Option[String] = None
      override def feature_RandomInt_rawSQL(a: String, b: String): String = s"floor(rand() * ($b - $a + 1) + $a)"

    given RandomFloat = new RandomFloat {}
    given RandomUUID = new RandomUUID {}
    // TODO now that we have precedence, fix the parenthesization rules for this!
    given RandomIntegerInInclusiveRange = new RandomIntegerInInclusiveRange {}

  object duckdb:
    given Dialect = new Dialect
        with QuotingIdentifiers.DuckdbBehavior
        with LimitAndOffset.Separate
        with StringLiteral.DuckdbBehavior
        with BooleanLiterals.UseTrueFalse:
      override def name(): String = "DuckDB Dialect"
      override val stringLengthByCharacters = "length"
      override val stringLengthByBytes = Seq("encode", "octet_length")
      override def feature_RandomUUID_functionName: String = "uuid"
      override def feature_RandomFloat_functionName: Option[String] = Some("random")
      override def feature_RandomFloat_rawSQL: Option[String] = None
      override def feature_RandomInt_rawSQL(a: String, b: String): String = s"floor(random() * ($b - $a + 1) + $a)::integer"

    given RandomFloat = new RandomFloat {}
    given RandomUUID = new RandomUUID {}
    // TODO now that we have precedence, fix the parenthesization rules for this!
    given RandomIntegerInInclusiveRange = new RandomIntegerInInclusiveRange {}
    given ReversibleStrings = new ReversibleStrings {}
