package tyql

import scala.annotation.elidable
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
  val stringLengthBytesNeedsEncodeFirst: Boolean = false

  val xorOperatorSupportedNatively = false

  def feature_RandomUUID_functionName: String = unsupportedFeature("RandomUUID")
  def feature_RandomFloat_functionName: Option[String] = throw new UnsupportedOperationException("RandomFloat")
  def feature_RandomFloat_rawSQL: Option[SqlSnippet] = throw new UnsupportedOperationException("RandomFloat")
  def feature_RandomInt_rawSQL: SqlSnippet = unsupportedFeature("RandomInt")

  def needsStringRepeatPolyfill: Boolean = false
  def needsStringLPadRPadPolyfill: Boolean = false

  def stringPositionFindingVia: String = "LOCATE"

  val nullSafeEqualityViaSpecialOperator: Boolean = false

  val booleanCast: String = "BOOLEAN"
  val integerCast: String = "INTEGER"
  val doubleCast: String = "DOUBLE PRECISION"
  val stringCast: String = "VARCHAR"

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
    given [T: ResultTag]: CanBeEqualed[T, T] = new CanBeEqualed[T, T] {}

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
      override def feature_RandomFloat_rawSQL: Option[SqlSnippet] = None
      override def feature_RandomInt_rawSQL: SqlSnippet =
        val a = ("a", Precedence.Concat)
        val b = ("b", Precedence.Concat)
        SqlSnippet(Precedence.Unary, snippet"(with randomIntParameters as (select $a as a, $b as b) select floor(random() * (b - a + 1) + a)::integer from randomIntParameters)")
      override def stringPositionFindingVia: String = "POSITION"

    given RandomUUID = new RandomUUID {}
    given RandomIntegerInInclusiveRange = new RandomIntegerInInclusiveRange {}
    given ReversibleStrings = new ReversibleStrings {}
    given [T: ResultTag]: CanBeEqualed[T, T] = new CanBeEqualed[T, T] {}
    // TODO later support more options here (?)
    given CanBeEqualed[Double, Int] = new CanBeEqualed[Double, Int] {}
    given CanBeEqualed[Int, Double] = new CanBeEqualed[Int, Double] {}


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
      override def feature_RandomFloat_rawSQL: Option[SqlSnippet] = None
      override def feature_RandomInt_rawSQL: SqlSnippet =
        val a = ("a", Precedence.Concat)
        val b = ("b", Precedence.Concat)
        SqlSnippet(Precedence.Unary, snippet"(with randomIntParameters as (select $a as a, $b as b) select floor(rand() * (b - a + 1) + a) from randomIntParameters)")
      override val nullSafeEqualityViaSpecialOperator: Boolean = true
      override val booleanCast: String = "SIGNED"
      override val integerCast: String = "DECIMAL"
      override val doubleCast: String = "DOUBLE"
      override val stringCast: String = "CHAR"

    given RandomUUID = new RandomUUID {}
    given RandomIntegerInInclusiveRange = new RandomIntegerInInclusiveRange {}
    given ReversibleStrings = new ReversibleStrings {}
    given [T1, T2]: CanBeEqualed[T1, T2] = new CanBeEqualed[T1, T2] {}

  object mariadb:
    // XXX MariaDB extends MySQL
    // XXX but you still have to redeclare the givens
    given Dialect = new mysql.MySQLDialect with QuotingIdentifiers.MariadbBehavior:
      override def name() = "MariaDB Dialect"

    given RandomUUID = mysql.given_RandomUUID
    given RandomIntegerInInclusiveRange = mysql.given_RandomIntegerInInclusiveRange
    given ReversibleStrings = mysql.given_ReversibleStrings
    given [T1, T2]: CanBeEqualed[T1, T2] = new CanBeEqualed[T1, T2] {}

  object sqlite:
    given Dialect = new Dialect
        with QuotingIdentifiers.SqliteBehavior
        with LimitAndOffset.Separate
        with StringLiteral.AnsiSingleQuote
        with BooleanLiterals.UseTrueFalse:
      def name() = "SQLite Dialect"
      override val stringLengthByCharacters = "length"
      override def feature_RandomFloat_functionName: Option[String] = None
      override def feature_RandomFloat_rawSQL: Option[SqlSnippet] = Some(SqlSnippet(Precedence.Unary, snippet"(0.5 - RANDOM() / CAST(-9223372036854775808 AS REAL) / 2)"))
      override def feature_RandomInt_rawSQL: SqlSnippet =
        val a = ("a", Precedence.Concat)
        val b = ("b", Precedence.Concat)
        SqlSnippet(Precedence.Unary, snippet"(with randomIntParameters as (select $a as a, $b as b) select cast(abs(random() % (b - a + 1) + a) as integer) from randomIntParameters)")
      override def needsStringRepeatPolyfill: Boolean = true
      override def needsStringLPadRPadPolyfill: Boolean = true
      override def stringPositionFindingVia: String = "INSTR"

    given RandomIntegerInInclusiveRange = new RandomIntegerInInclusiveRange {}
    given ReversibleStrings = new ReversibleStrings {}
    given [T1, T2]: CanBeEqualed[T1, T2] = new CanBeEqualed[T1, T2] {}

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
      override def feature_RandomFloat_rawSQL: Option[SqlSnippet] = None
      override def feature_RandomInt_rawSQL: SqlSnippet =
        val a = ("a", Precedence.Concat)
        val b = ("b", Precedence.Concat)
        SqlSnippet(Precedence.Unary, snippet"(with randomIntParameters as (select $a as a, $b as b) select floor(rand() * (b - a + 1) + a) from randomIntParameters)")

    given RandomUUID = new RandomUUID {}
    given RandomIntegerInInclusiveRange = new RandomIntegerInInclusiveRange {}
    given [T: ResultTag]: CanBeEqualed[T, T] = new CanBeEqualed[T, T] {}
    // TODO later support more options here (?)
    given CanBeEqualed[Double, Int] = new CanBeEqualed[Double, Int] {}
    given CanBeEqualed[Int, Double] = new CanBeEqualed[Int, Double] {}


  object duckdb:
    given Dialect = new Dialect
        with QuotingIdentifiers.DuckdbBehavior
        with LimitAndOffset.Separate
        with StringLiteral.DuckdbBehavior
        with BooleanLiterals.UseTrueFalse:
      override def name(): String = "DuckDB Dialect"
      override val stringLengthByCharacters = "length"
      override val stringLengthBytesNeedsEncodeFirst = true
      override def feature_RandomUUID_functionName: String = "uuid"
      override def feature_RandomFloat_functionName: Option[String] = Some("random")
      override def feature_RandomFloat_rawSQL: Option[SqlSnippet] = None
      override def feature_RandomInt_rawSQL: SqlSnippet =
        val a = ("a", Precedence.Concat)
        val b = ("b", Precedence.Concat)
        SqlSnippet(Precedence.Unary, snippet"(with randomIntParameters as (select $a as a, $b as b) select floor(random() * (b - a + 1) + a)::integer from randomIntParameters)")
      override def stringPositionFindingVia: String = "POSITION"

    given RandomUUID = new RandomUUID {}
    given RandomIntegerInInclusiveRange = new RandomIntegerInInclusiveRange {}
    given ReversibleStrings = new ReversibleStrings {}
    given [T: ResultTag]: CanBeEqualed[T, T] = new CanBeEqualed[T, T] {}
    // TODO later support more options here (?)
    given CanBeEqualed[Double, Int] = new CanBeEqualed[Double, Int] {}
    given CanBeEqualed[Int, Double] = new CanBeEqualed[Int, Double] {}


/**
 * Some dialect operations are defined by polyfills to provide consistent semantics across dialects.
 * This is tracked when the documentation is generated, and for this purpose it should be enabled, but
 * since it also has a small performance impact, for normal releases it should be disabled.
 */
inline val shouldTrackPolyfillUsage = false
var wasPolyfillUsed: Boolean = false

inline def polyfillWasUsed(): Unit =
  inline if shouldTrackPolyfillUsage then
    wasPolyfillUsed = true
