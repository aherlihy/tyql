package tyql

// Mostly compatible with https://www.postgresql.org/docs/current/sql-syntax-lexical.html#SQL-PRECEDENCE
object Precedence {
  val Literal = 100 // literals, identifiers
  val ListOps = 95 // list_append, list_prepend, list_contains
  val Unary = 90 // NOT, EXIST, etc
  val Multiplicative = 80 // *, /
  val Additive = 70 // +, -
  val Comparison = 60 // =, <>, <, >, <=, >=
  val And = 50 // AND
  val Or = 40 // OR
  val Concat = 10 // , in select clause
  val Default = 0
}
