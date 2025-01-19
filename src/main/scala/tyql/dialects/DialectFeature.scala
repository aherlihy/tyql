package tyql

trait DialectFeature

object DialectFeature:
  @annotation.implicitNotFound(
    "RandomUUID operation can be expressed only in dialects other than SQLite. Please use supported dialect."
  )
  trait RandomUUID extends DialectFeature
  trait RandomIntegerInInclusiveRange extends DialectFeature

  @annotation.implicitNotFound(
    "Strings can be reversed efficiently only in dialects other than H2. Please use supported dialect."
  )
  trait ReversibleStrings extends DialectFeature

  @annotation.implicitNotFound(
    "To use non-simple types inside IN clause, use dialect other than DuckDB. Please use supported dialect."
  )
  trait INCanHandleRows extends DialectFeature

  @annotation.implicitNotFound(
    "LIMIT clauses in DELETE queries are supported only in MySQL, MariaDB and H2 dialects. Please use supported dialect."
  )
  trait AcceptsLimitInDeletes extends DialectFeature
  @annotation.implicitNotFound(
    "ORDER BY clauses in DELETE queries are supported only in MySQL, MariaDB dialects. Please use supported dialect."
  )
  trait AcceptsOrderByInDeletes extends DialectFeature

  @annotation.implicitNotFound(
    "LIMIT and ORDER BY clauses in UPDATE queries are supported only in MySQL, MariaDB and H2 dialects. Please use supported dialect."
  )
  trait AcceptsLimitAndOrderByInUpdates extends DialectFeature

  @annotation.implicitNotFound(
    "Only Postgres, DuckDB, and H2 support singledimensional arrays. Please use supported dialect, e.g. with `import tyql.dialects.postgresql.given`."
  )
  trait HomogenousArraysOf1D extends DialectFeature
