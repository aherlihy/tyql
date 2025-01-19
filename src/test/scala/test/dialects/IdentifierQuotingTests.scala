package test.integration.identifierquoting

import munit.FunSuite
import tyql.Dialect

class IdentifierQuotingTests extends FunSuite {
  private val dialects = Set(
    Dialect.ansi.given_Dialect,
    Dialect.postgresql.given_Dialect,
    Dialect.mysql.given_Dialect,
    Dialect.mariadb.given_Dialect,
    Dialect.h2.given_Dialect,
    Dialect.duckdb.given_Dialect,
    Dialect.sqlite.given_Dialect,
  )

  test("common names are not quoted") {
    for (d <- dialects; e <- Set("sum", "min", "max", "avg", "count")) {
      assertEquals(d.quoteIdentifier(e), e)
      assertEquals(d.quoteIdentifier(e.toUpperCase), e.toUpperCase)
    }
  }

  test("names with spaces are quoted") {
    for (d <- dialects; e <- Set("a b", "c d", "a b d")) {
      assertNotEquals(d.quoteIdentifier(e), e)
      assertNotEquals(d.quoteIdentifier(e.toUpperCase), e.toUpperCase)
      assertEquals(d.quoteIdentifier(e).length, e.length + 2)
      assertEquals(d.quoteIdentifier(e.toUpperCase).length, e.toUpperCase.length + 2)
    }
  }

  test("names starting with numbers") {
    val needQuoting = Set("1abc", "12r", "0_")
    val doNotNeedQuoting = Set("abc1", "r12", "_0")
    for (d <- dialects; e <- needQuoting) {
      assertNotEquals(d.quoteIdentifier(e), e)
      assertEquals(d.quoteIdentifier(e).length, e.length + 2)
    }
    for (d <- dialects; e <- doNotNeedQuoting) {
      assertEquals(d.quoteIdentifier(e), e)
    }
  }

  test("reserved keywords are quoted") {
    val needQuoting = Set("select", "where", "from")
    for (d <- dialects; e <- needQuoting) {
      assertNotEquals(d.quoteIdentifier(e), e)
      assertEquals(d.quoteIdentifier(e).length, e.length + 2)
    }
  }

  test("different reserved keywords per database") {
    assertEquals(Dialect.postgresql.given_Dialect.quoteIdentifier("user"), "\"user\"")
    assertEquals(Dialect.sqlite.given_Dialect.quoteIdentifier("user"), "user")
  }

  test("escaping inner quotes") {
    assertEquals(Dialect.mariadb.given_Dialect.quoteIdentifier("a``b`c"), "`a````b``c`")
    assertEquals(Dialect.mysql.given_Dialect.quoteIdentifier("a``b`c"), "`a````b``c`")

    for (d <- dialects -- Set(Dialect.mariadb.given_Dialect, Dialect.mysql.given_Dialect)) {
      assertEquals(d.quoteIdentifier("a\"\"b\"c"), "\"a\"\"\"\"b\"\"c\"")
    }
  }
}
