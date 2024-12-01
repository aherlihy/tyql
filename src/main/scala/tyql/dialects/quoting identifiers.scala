package tyql

object QuotingIdentifiers:
  // TODO what about `group`, `order`? Do we need to split it into alias context and column name context?
  private val keywordsNotToBeQuotedRegardlessOfStandard: Set[String] = Set(
    "SUM", "COUNT", "MIN", "MAX", "AVG", "VALUE", "KEY", "TYPE", "DATE", "TIME", "TIMESTAMP", "OF"
  )

  private def needsQuoting(reservedKeywords: Set[String], id: String): Boolean =
    // TODO think again more carefully when is `*` allowed and when it's not
    (
      (reservedKeywords.contains(id.toUpperCase)
        && !keywordsNotToBeQuotedRegardlessOfStandard.contains(id.toUpperCase))
    ||
      (!id.matches("[a-zA-Z_][a-zA-Z0-9_]*")
        && id != "*")
    )

  trait DoubleQuotes extends Dialect:
    def quoteIdentifier(id: String): String =
      if needsQuoting(reservedKeywords, id) then s""""${id.replace("\"", "\"\"")}"""" else id

  trait Backticks extends Dialect:
    def quoteIdentifier(id: String): String =
      if needsQuoting(reservedKeywords, id) then s"""`${id.replace("`", "``")}`""" else id


  trait AnsiBehavior extends DoubleQuotes:
    // keywords list is also maintained by PostgreSQL at https://www.postgresql.org/docs/current/sql-keywords-appendix.html
    // last extracted 2024-11-15 using Claude Sonnet 3.5 v20241022
    override protected val reservedKeywords: Set[String] = Set(
      "ABSOLUTE", "ACTION", "ADD", "ALL", "ALLOCATE", "ALTER", "AND", "ANY", "ARE", "AS",
      "ASC", "ASSERTION", "AT", "AUTHORIZATION", "AVG",
      "BEGIN", "BETWEEN", "BIT", "BY",
      "CASCADE", "CASCADED", "CASE", "CAST", "CATALOG", "CHAR", "CHARACTER", "CHECK",
      "CLOSE", "COALESCE", "COLLATE", "COLLATION", "COLUMN", "COMMIT", "CONNECT",
      "CONNECTION", "CONSTRAINT", "CONSTRAINTS", "CONTINUE", "CONVERT", "CORRESPONDING",
      "COUNT", "CREATE", "CROSS", "CURRENT", "CURRENT_DATE", "CURRENT_TIME",
      "CURRENT_TIMESTAMP", "CURRENT_USER", "CURSOR",
      "DATE", "DAY", "DEALLOCATE", "DEC", "DECIMAL", "DECLARE", "DEFAULT", "DEFERRABLE",
      "DEFERRED", "DELETE", "DESC", "DESCRIBE", "DESCRIPTOR", "DIAGNOSTICS", "DISCONNECT",
      "DISTINCT", "DOMAIN", "DOUBLE", "DROP",
      "ESCAPE", "EXCEPT", "EXCEPTION",
      "EXEC", "EXECUTE", "EXISTS", "EXTERNAL", "EXTRACT",
      "FALSE", "FETCH", "FIRST", "FLOAT", "FOR", "FOREIGN", "FOUND", "FROM", "FULL",
      "GET", "GLOBAL", "GO", "GOTO", "GRANT", "GROUP",
      "HAVING", "HOUR",
      "IDENTITY", "IMMEDIATE", "IN", "INDICATOR", "INITIALLY", "INNER", "INPUT",
      "INSENSITIVE", "INSERT", "INT", "INTEGER", "INTERSECT", "INTERVAL", "INTO", "IS",
      "ISOLATION",
      "JOIN",
      "KEY",
      "LANGUAGE", "LAST", "LEADING", "LEFT", "LEVEL", "LIKE", "LOCAL", "LOWER",
      "MATCH", "MAX", "MIN", "MINUTE", "MODULE", "MONTH",
      "NAMES", "NATIONAL", "NATURAL", "NCHAR", "NEXT", "NO", "NOT", "NULL", "NUMERIC",
      "OCTET_LENGTH", "OF", "ON", "ONLY", "OPEN", "OPTION", "OR", "ORDER", "OUTER",
      "OUTPUT", "OVERLAPS",
      "PAD", "PARTIAL", "POSITION", "PRECISION", "PREPARE", "PRESERVE", "PRIMARY", "PRIOR",
      "PRIVILEGES", "PROCEDURE", "PUBLIC",
      "READ", "REAL", "REFERENCES", "RELATIVE", "RESTRICT", "REVOKE", "RIGHT", "ROLLBACK",
      "ROWS",
      "SCHEMA", "SCROLL", "SECOND", "SECTION", "SELECT", "SESSION", "SESSION_USER", "SET",
      "SIZE", "SMALLINT", "SOME", "SPACE", "SQL", "SQLCODE", "SQLERROR", "SQLSTATE",
      "SUBSTRING", "SUM", "SYSTEM_USER",
      "TABLE", "TEMPORARY", "THEN", "TIME", "TIMESTAMP", "TIMEZONE_HOUR", "TIMEZONE_MINUTE",
      "TO", "TRAILING", "TRANSACTION", "TRANSLATE", "TRANSLATION", "TRIM", "TRUE",
      "UNION", "UNIQUE", "UNKNOWN", "UPDATE", "UPPER", "USAGE", "USER", "USING",
      "VALUE", "VALUES", "VARCHAR", "VARYING", "VIEW",
      "WHEN", "WHENEVER", "WHERE", "WITH", "WORK", "WRITE",
      "YEAR",
      "ZONE"
    )

  trait PostgresqlBehavior extends DoubleQuotes:
    // https://www.postgresql.org/docs/current/sql-keywords-appendix.html
    // last extracted 2024-11-15 using Claude Sonnet 3.5 v20241022
    override protected val reservedKeywords: Set[String] = Set(
      "ANALYSE", "ANALYZE", // Note: both ANALYSE and ANALYZE are reserved in PostgreSQL
      "ALL", "AND", "ANY", "AS", "ASC", "ASYMMETRIC", "BOTH", "CASE", "CAST", "CHECK",
      "COLLATE", "COLUMN", "CONSTRAINT", "CREATE", "DESC", "DISTINCT", "DO", "ELSE", "END",
      "EXCEPT", "FALSE", "FOR", "FROM", "GRANT", "GROUP", "HAVING", "IN", "INITIALLY", "INTERSECT",
      "INTO", "LATERAL", "LEADING", "LIMIT", "NOT", "NULL", "OFFSET", "ON", "ONLY", "OR", "ORDER",
      "PLACING", "PRIMARY", "REFERENCES", "RETURNING", "SELECT", "SESSION_USER", "SOME", "SYMMETRIC",
      "TABLE", "THEN", "TO", "TRAILING", "TRUE", "UNION", "UNIQUE", "USER", "USING", "VARIADIC",
      "WHEN", "WHERE", "WINDOW", "WITH"
    )


  trait MysqlBehavior extends Backticks:
    // https://dev.mysql.com/doc/refman/8.0/en/keywords.html
    // last extracted 2024-11-15 using Claude Sonnet 3.5 v20241022
    override protected val reservedKeywords: Set[String] = Set(
      "ACCESSIBLE", "ADD", "ALL", "ALTER", "ANALYZE", "AND", "AS", "ASC", "ASENSITIVE",
      "BEFORE", "BETWEEN", "BIGINT", "BINARY", "BLOB", "BOTH", "BY",
      "CALL", "CASCADE", "CASE", "CHANGE", "CHAR", "CHARACTER", "CHECK", "COLLATE", "COLUMN",
      "CONDITION", "CONSTRAINT", "CONTINUE", "CONVERT", "CREATE", "CROSS", "CUBE", "CUME_DIST",
      "CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP", "CURRENT_USER", "CURSOR",
      "DATABASE", "DATABASES", "DAY_HOUR", "DAY_MICROSECOND", "DAY_MINUTE", "DAY_SECOND",
      "DEC", "DECIMAL", "DECLARE", "DEFAULT", "DELAYED", "DELETE", "DENSE_RANK", "DESC",
      "DESCRIBE", "DETERMINISTIC", "DISTINCT", "DISTINCTROW", "DIV", "DOUBLE", "DROP", "DUAL",
      "EACH", "ELSE", "ELSEIF", "EMPTY", "ENCLOSED", "ESCAPED", "EXISTS", "EXIT",
      "EXPLAIN", "FALSE", "FETCH", "FIRST_VALUE", "FLOAT", "FLOAT4", "FLOAT8", "FOR",
      "FORCE", "FOREIGN", "FROM", "FULLTEXT", "FUNCTION", "GENERATED", "GET",
      "GRANT", "GROUP", "GROUPING", "GROUPS", "HAVING", "HIGH_PRIORITY",
      "HOUR_MICROSECOND", "HOUR_MINUTE", "HOUR_SECOND",
      "IF", "IGNORE", "IN", "INDEX", "INFILE", "INNER", "INOUT", "INSENSITIVE", "INSERT",
      "INT", "INT1", "INT2", "INT3", "INT4", "INT8", "INTEGER", "INTERVAL", "INTO", "IO_AFTER_GTIDS",
      "IO_BEFORE_GTIDS", "IS", "ITERATE",
      "JOIN", "JSON_TABLE", "KEY", "KEYS", "KILL",
      "LAG", "LAST_VALUE", "LATERAL", "LEAD", "LEADING", "LEAVE", "LEFT", "LIKE", "LIMIT",
      "LINEAR", "LINES", "LOAD", "LOCALTIME", "LOCALTIMESTAMP", "LOCK", "LONG", "LONGBLOB",
      "LONGTEXT", "LOOP", "LOW_PRIORITY",
      "MASTER_BIND", "MASTER_SSL_VERIFY_SERVER_CERT", "MATCH", "MAXVALUE", "MEDIUMBLOB",
      "MEDIUMINT", "MEDIUMTEXT", "MIDDLEINT", "MINUTE_MICROSECOND", "MINUTE_SECOND", "MOD",
      "MODIFIES",
      "NATURAL", "NOT", "NO_WRITE_TO_BINLOG", "NTH_VALUE", "NTILE", "NULL", "NUMERIC",
      "OF", "ON", "OPTIMIZE", "OPTIMIZER_COSTS", "OPTION", "OPTIONALLY", "OR", "ORDER", "OUT",
      "OUTER", "OUTFILE", "OVER",
      "PARTITION", "PERCENT_RANK", "PRECISION", "PRIMARY", "PROCEDURE", "PURGE",
      "RANGE", "RANK", "READ", "READS", "READ_WRITE", "REAL", "RECURSIVE", "REFERENCES", "REGEXP",
      "RELEASE", "RENAME", "REPEAT", "REPLACE", "REQUIRE", "RESIGNAL", "RESTRICT", "RETURN",
      "REVOKE", "RIGHT", "RLIKE", "ROW", "ROWS",
      "SCHEMA", "SCHEMAS", "SECOND_MICROSECOND", "SELECT", "SENSITIVE", "SEPARATOR", "SET",
      "SHOW", "SIGNAL", "SMALLINT", "SPATIAL", "SPECIFIC", "SQL", "SQLEXCEPTION", "SQLSTATE",
      "SQLWARNING", "SQL_BIG_RESULT", "SQL_CALC_FOUND_ROWS", "SQL_SMALL_RESULT", "SSL",
      "STARTING", "STORED", "STRAIGHT_JOIN", "SYSTEM",
      "TABLE", "TERMINATED", "THEN", "TINYBLOB", "TINYINT", "TINYTEXT", "TO", "TRAILING",
      "TRIGGER", "TRUE",
      "UNDO", "UNION", "UNIQUE", "UNLOCK", "UNSIGNED", "UPDATE", "USAGE", "USE", "USING",
      "UTC_DATE", "UTC_TIME", "UTC_TIMESTAMP",
      "VALUES", "VARBINARY", "VARCHAR", "VARCHARACTER", "VARYING", "VIRTUAL",
      "WHEN", "WHERE", "WHILE", "WINDOW", "WITH", "WRITE",
      "XOR", "YEAR_MONTH", "ZEROFILL"
    )

  trait MariadbBehavior extends Backticks:
    // https://mariadb.com/kb/en/reserved-words/
    // last extracted 2024-11-15 using Claude Sonnet 3.5 v20241022
    override protected val reservedKeywords: Set[String] = Set(
      "ACCESSIBLE", "ADD", "ALL", "ALTER", "ANALYZE", "AND", "AS", "ASC", "ASENSITIVE",
      "BEFORE", "BETWEEN", "BIGINT", "BINARY", "BLOB", "BOTH", "BY",
      "CALL", "CASCADE", "CASE", "CHANGE", "CHAR", "CHARACTER", "CHECK", "COLLATE",
      "COLUMN", "CONDITION", "CONSTRAINT", "CONTINUE", "CONVERT", "CREATE", "CROSS",
      "CURRENT_DATE", "CURRENT_ROLE", "CURRENT_TIME", "CURRENT_TIMESTAMP", "CURRENT_USER", "CURSOR",
      "DATABASE", "DATABASES", "DAY_HOUR", "DAY_MICROSECOND", "DAY_MINUTE", "DAY_SECOND",
      "DEC", "DECIMAL", "DECLARE", "DEFAULT", "DELAYED", "DELETE", "DELETE_DOMAIN_ID", "DESC",
      "DESCRIBE", "DETERMINISTIC", "DISTINCT", "DISTINCTROW", "DIV", "DO_DOMAIN_IDS", "DOUBLE", "DROP", "DUAL",
      "EACH", "ELSE", "ELSEIF", "ENCLOSED", "ESCAPED", "EXCEPT", "EXISTS", "EXIT", "EXPLAIN",
      "FALSE", "FETCH", "FLOAT", "FLOAT4", "FLOAT8", "FOR", "FORCE", "FOREIGN", "FROM", "FULLTEXT",
      "GENERAL", "GRANT", "GROUP",
      "HAVING", "HIGH_PRIORITY", "HOUR_MICROSECOND", "HOUR_MINUTE", "HOUR_SECOND",
      "IF", "IGNORE", "IGNORE_DOMAIN_IDS", "IGNORE_SERVER_IDS", "IN", "INDEX", "INFILE", "INNER",
      "INOUT", "INSENSITIVE", "INSERT", "INT", "INT1", "INT2", "INT3", "INT4", "INT8",
      "INTEGER", "INTERSECT", "INTERVAL", "INTO", "IS", "ITERATE",
      "JOIN",
      "KEY", "KEYS", "KILL",
      "LEADING", "LEAVE", "LEFT", "LIKE", "LIMIT", "LINEAR", "LINES", "LOAD", "LOCALTIME",
      "LOCALTIMESTAMP", "LOCK", "LONG", "LONGBLOB", "LONGTEXT", "LOOP", "LOW_PRIORITY",
      "MASTER_HEARTBEAT_PERIOD", "MASTER_SSL_VERIFY_SERVER_CERT", "MATCH", "MAXVALUE",
      "MEDIUMBLOB", "MEDIUMINT", "MEDIUMTEXT", "MIDDLEINT", "MINUTE_MICROSECOND",
      "MINUTE_SECOND", "MOD", "MODIFIES",
      "NATURAL", "NOT", "NO_WRITE_TO_BINLOG", "NULL", "NUMERIC",
      "OFFSET", "ON", "OPTIMIZE", "OPTION", "OPTIONALLY", "OR", "ORDER", "OUT", "OUTER",
      "OUTFILE", "OVER",
      "PAGE_CHECKSUM", "PARSE_VCOL_EXPR", "PARTITION", "PRECISION", "PRIMARY", "PROCEDURE", "PURGE",
      "RANGE", "READ", "READS", "READ_WRITE", "REAL", "RECURSIVE", "REF_SYSTEM_ID",
      "REFERENCES", "REGEXP", "RELEASE", "RENAME", "REPEAT", "REPLACE", "REQUIRE",
      "RESIGNAL", "RESTRICT", "RETURN", "RETURNING", "REVOKE", "RIGHT", "RLIKE", "ROW_NUMBER", "ROWS",
      "SCHEMA", "SCHEMAS", "SECOND_MICROSECOND", "SELECT", "SENSITIVE", "SEPARATOR", "SET",
      "SHOW", "SIGNAL", "SLOW", "SMALLINT", "SPATIAL", "SPECIFIC", "SQL", "SQLEXCEPTION",
      "SQLSTATE", "SQLWARNING", "SQL_BIG_RESULT", "SQL_CALC_FOUND_ROWS", "SQL_SMALL_RESULT",
      "SSL", "STARTING", "STATS_AUTO_RECALC", "STATS_PERSISTENT", "STATS_SAMPLE_PAGES",
      "STRAIGHT_JOIN",
      "TABLE", "TERMINATED", "THEN", "TINYBLOB", "TINYINT", "TINYTEXT", "TO", "TRAILING",
      "TRIGGER", "TRUE",
      "UNDO", "UNION", "UNIQUE", "UNLOCK", "UNSIGNED", "UPDATE", "USAGE", "USE", "USING",
      "UTC_DATE", "UTC_TIME", "UTC_TIMESTAMP",
      "VALUES", "VARBINARY", "VARCHAR", "VARCHARACTER", "VARYING",
      "WHEN", "WHERE", "WHILE", "WINDOW", "WITH", "WRITE",
      "XOR",
      "YEAR_MONTH",
      "ZEROFILL"
    )

  trait SqliteBehavior extends DoubleQuotes:
    // https://www.sqlite.org/lang_keywords.html
    // last extracted 2024-11-15 using Claude Sonnet 3.5 v20241022
    override protected val reservedKeywords: Set[String] = Set(
      "ABORT", "ACTION", "ADD", "AFTER", "ALL", "ALTER", "ALWAYS", "ANALYZE", "AND", "AS",
      "ASC", "ATTACH", "AUTOINCREMENT", "BEFORE", "BEGIN", "BETWEEN", "BY", "CASCADE",
      "CASE", "CAST", "CHECK", "COLLATE", "COLUMN", "COMMIT", "CONFLICT", "CONSTRAINT",
      "CREATE", "CROSS", "CURRENT", "CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP",
      "DATABASE", "DEFAULT", "DEFERRABLE", "DEFERRED", "DELETE", "DESC", "DETACH",
      "DISTINCT", "DO", "DROP", "EACH", "ELSE", "END", "ESCAPE", "EXCEPT", "EXCLUDE",
      "EXCLUSIVE", "EXISTS", "EXPLAIN", "FAIL", "FILTER", "FIRST", "FOLLOWING", "FOR",
      "FOREIGN", "FROM", "FULL", "GENERATED", "GLOB", "GROUP", "GROUPS", "HAVING", "IF",
      "IGNORE", "IMMEDIATE", "IN", "INDEX", "INDEXED", "INITIALLY", "INNER", "INSERT",
      "INSTEAD", "INTERSECT", "INTO", "IS", "ISNULL", "JOIN", "KEY", "LAST", "LEFT",
      "LIKE", "LIMIT", "MATCH", "MATERIALIZED", "NATURAL", "NO", "NOT", "NOTHING",
      "NOTNULL", "NULL", "NULLS", "OF", "OFFSET", "ON", "OR", "ORDER", "OTHERS", "OUTER",
      "OVER", "PARTITION", "PLAN", "PRAGMA", "PRECEDING", "PRIMARY", "QUERY", "RAISE",
      "RANGE", "RECURSIVE", "REFERENCES", "REGEXP", "REINDEX", "RELEASE", "RENAME",
      "REPLACE", "RESTRICT", "RETURNING", "RIGHT", "ROLLBACK", "ROW", "ROWS", "SAVEPOINT",
      "SELECT", "SET", "TABLE", "TEMP", "TEMPORARY", "THEN", "TIES", "TO", "TRANSACTION",
      "TRIGGER", "UNBOUNDED", "UNION", "UNIQUE", "UPDATE", "USING", "VACUUM", "VALUES",
      "VIEW", "VIRTUAL", "WHEN", "WHERE", "WINDOW", "WITH", "WITHOUT"
    )

  trait H2Behavior extends DoubleQuotes:
    // https://h2database.com/html/grammar.html
    // last extracted 2024-11-15 using Claude Sonnet 3.5 v20241022
    override protected val reservedKeywords: Set[String] = Set(
      // Basic SQL keywords
      "ALL", "AND", "ANY", "ARRAY", "AS", "ASC", "BETWEEN", "BOTH", "CASE", "CAST",
      "CHECK", "CONSTRAINT", "CROSS", "CURRENT", "CURRENT_DATE", "CURRENT_TIME",
      "CURRENT_TIMESTAMP", "CURRENT_USER", "DISTINCT", "EXCEPT", "EXISTS", "FALSE",
      "FETCH", "FOR", "FOREIGN", "FROM", "FULL", "GROUP", "HAVING", "IF", "ILIKE",
      "IN", "INNER", "INTERSECT", "INTERVAL", "IS", "JOIN", "LEADING", "LEFT", "LIKE",
      "LIMIT", "LOCALTIME", "LOCALTIMESTAMP", "MINUS", "NATURAL", "NOT", "NULL",
      "OFFSET", "ON", "OR", "ORDER", "PRIMARY", "QUALIFY", "REGEXP", "RIGHT", "ROW",
      "SELECT", "SYSDATE", "SYSTIME", "SYSTIMESTAMP", "TABLE", "TODAY", "TOP", "TRAILING",
      "TRUE", "UNION", "UNIQUE", "UNKNOWN", "USING", "VALUES", "WHERE", "WINDOW", "WITH",
      "DELETE", "INSERT", "MERGE", "REPLACE", "UPDATE", "UPSERT",
      "ADD", "ALTER", "COLUMN", "CREATE", "DATABASE", "DROP", "INDEX", "SCHEMA", "SET",
      "TABLE", "VIEW",
      "COMMIT", "ROLLBACK", "SAVEPOINT", "START",
      "_ROWID_", "AUTOCOMMIT", "CACHED", "CHECKPOINT", "EXCLUSIVE", "IGNORECASE",
      "IFEXISTS", "IFNOTEXISTS", "MEMORY", "MINUS", "NEXT", "OF", "OFF", "PASSWORD",
      "READONLY", "REFERENTIAL_INTEGRITY", "REUSE", "ROWNUM", "SEQUENCE", "TEMP",
      "TEMPORARY", "TRIGGER", "VALUE", "YEAR",
      "BINARY", "BLOB", "BOOLEAN", "CHAR", "CHARACTER", "CLOB", "DATE", "DECIMAL",
      "DOUBLE", "FLOAT", "INT", "INTEGER", "LONG", "NUMBER", "NUMERIC", "REAL",
      "SMALLINT", "TIME", "TIMESTAMP", "TINYINT", "VARCHAR"
    )

  trait DuckdbBehavior extends DoubleQuotes:
    // SELECT keyword_name FROM duckdb_keywords() WHERE keyword_category IN ('reserved', 'type_function')
    // last extracted 2024-11-15 from DuckDB REPL v1.1.3 19864453f7
    override protected val reservedKeywords: Set[String] = Set(
      "ALL", "ANALYSE", "ANALYZE", "AND", "ANY", "ARRAY", "AS", "ASC",
      "ASYMMETRIC", "BOTH", "CASE", "CAST", "CHECK", "COLLATE", "COLUMN",
      "CONSTRAINT", "CREATE", "DEFAULT", "DEFERRABLE", "DESC", "DISTINCT",
      "DO", "ELSE", "END", "EXCEPT", "FALSE", "FETCH", "FOR", "FOREIGN",
      "FROM", "GRANT", "GROUP", "HAVING", "IN", "INITIALLY", "INTERSECT",
      "INTO", "LATERAL", "LEADING", "LIMIT", "NOT", "NULL", "OFFSET",
      "ON", "ONLY", "OR", "ORDER", "PIVOT", "PIVOT_LONGER", "PIVOT_WIDER",
      "PLACING", "PRIMARY", "REFERENCES", "RETURNING", "SELECT", "SHOW",
      "SOME", "SYMMETRIC", "TABLE", "THEN", "TO", "TRAILING", "TRUE",
      "UNION", "UNIQUE", "UNPIVOT", "USING", "VARIADIC", "WHEN", "WHERE",
      "WINDOW", "WITH"
    ) ++ Set(
      "ANTI", "ASOF", "AUTHORIZATION", "BINARY", "CROSS", "FULL", "ILIKE",
      "INNER", "IS", "ISNULL", "JOIN", "LEFT", "LIKE", "MAP", "NATURAL",
      "NOTNULL", "OUTER", "OVERLAPS", "POSITIONAL", "RIGHT", "SEMI",
      "SIMILAR", "STRUCT", "TABLESAMPLE", "TRY_CAST", "VERBOSE"
    )
