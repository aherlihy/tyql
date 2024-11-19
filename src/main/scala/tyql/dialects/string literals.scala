package tyql

object StringLiteral:
  // XXX for now it's impossible to input things like \u4F60. Unclear if worth implementing since we're in Scala.
  // XXX unclear how H2 behaves. It appears to follow the ANSI standard?
  // XXX TODO make sure this plays nicely with LIKE. Maybe we will allow .like_pattern and .like_literal?

  private def handleLiteralPatterns(insideLikePattern: Boolean, in: String): (String, Boolean) =
    // ESCAPE is needed for ANSI, H2, DuckDB, not needed in PostgreSQL, SQLite
    // still, it's safer to emit since all these backend have configuration options
    // MySQL, MariaDB REJECT the ESCAPE keyword in LIKE patterns
    if insideLikePattern && in.exists(c => c == Dialect.literal_percent || c == Dialect.literal_underscore) then
      (in.replace(Dialect.literal_percent.toString, "\\%").replace(Dialect.literal_underscore.toString, "\\_"), true)
    else
      (in.replace(Dialect.literal_percent, '%').replace(Dialect.literal_underscore, '_'), false)

  trait AnsiSingleQuote extends Dialect:
    // last updated 2024-11-15 using Claude Sonnet 3.5 v20241022
    // https://www.sqlite.org/lang_expr.html
    // https://www.postgresql.org/docs/current/sql-syntax-lexical.html#SQL-SYNTAX-STRINGS
    override def quoteStringLiteral(lit: String, insideLikePattern: Boolean): String =
      val (in, shouldAddEscape) = handleLiteralPatterns(insideLikePattern, lit)
      val out = "'" + in.replace("'", "''") + "'"
      if shouldAddEscape then s"$out ESCAPE '\\'" else out

  trait PostgresqlBehavior extends Dialect:
    // last updated 2024-11-15 using Claude Sonnet 3.5 v20241022
    // https://www.postgresql.org/docs/current/sql-syntax-lexical.html
    override def quoteStringLiteral(lit: String, insideLikePattern: Boolean): String =
      val (in, shouldAddEscape) = handleLiteralPatterns(insideLikePattern, lit)
      val out = if in.exists("\n\r\t\b\f\\".contains) then
        "E'" + in.replace("\\", "\\\\")
                 .replace("'", "\\'")
                 .replace("\b", "\\b")
                 .replace("\f", "\\f")
                 .replace("\n", "\\n")
                 .replace("\r", "\\r")
                 .replace("\t", "\\t") + "'"
      else
        "'" + in.replace("'", "''") + "'"
      if shouldAddEscape then s"$out ESCAPE '\\'" else out

  trait MysqlBehavior extends Dialect:
    // last updated 2024-11-15 using Claude Sonnet 3.5 v20241022
    // https://dev.mysql.com/doc/refman/8.4/en/string-literals.html
    // https://mariadb.com/kb/en/string-literals/
    // XXX _ and % have different meaning in LIKE strings. For now we never escape them,
    //   but this means that you cannot encode a literal % in the pattern.
    override def quoteStringLiteral(lit: String, insideLikePattern: Boolean): String =
      val (in, shouldAddEscape) = handleLiteralPatterns(insideLikePattern, lit)
      val out = "'" + in.replace("\\", "\\\\")
                        .replace("\u0000", "\\0")
                        .replace("'", "\\'")
                        .replace("\"", "\\\"")
                        .replace("\b", "\\b")
                        .replace("\n", "\\n")
                        .replace("\r", "\\r")
                        .replace("\t", "\\t")
                        .replace("\u001A", "\\Z") + "'"
      out // ignore `ESCAPE` since MySQL/MariaDB do not support it

  trait DuckdbBehavior extends Dialect:
    // last updated 2024-11-15 using Claude Sonnet 3.5 v20241022
    // https://duckdb.org/docs/sql/data_types/literal_types.html#string-literals
    override def quoteStringLiteral(lit: String, insideLikePattern: Boolean): String =
      val (in, shouldAddEscape) = handleLiteralPatterns(insideLikePattern, lit)
      val out = if in.exists("\n\r\t\b\f\\".contains) then
        "E'" + in.replace("\\", "\\\\")
                .replace("'", "''")  // different from PostgreSQL
                .replace("\b", "\\b")
                .replace("\f", "\\f")
                .replace("\n", "\\n")
                .replace("\r", "\\r")
                .replace("\t", "\\t") + "'"
      else
        "'" + in.replace("'", "''") + "'"
      if shouldAddEscape then s"$out ESCAPE '\\'" else out
