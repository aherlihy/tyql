package test.integration.stringliteralescaping

import munit.FunSuite
import test.expensiveTest
import tyql.Dialect
import test.withDB
import java.sql.{Connection, DriverManager}

class StringLiteralDBTest extends FunSuite {
  private def testStringLiteral(conn: Connection, input: String)(using dialect: Dialect) = {
    val quoted = dialect.quoteStringLiteral(input, insideLikePattern=false)
    val stmt = conn.createStatement()
    val rs = stmt.executeQuery(s"SELECT $quoted as str")
    assert(rs.next())
    assertEquals(rs.getString("str"), input)
  }

  val interestingStrings = List(
    "a'b",
    "a\"b",
    "a\\b",
    "a\bb",     // Backspace
    "a\fb",
    "a\nb",
    "a\rb",
    "a\tb",
    "a\u001Ab", // Ctrl+Z
    "a%b",      // LIKE wildcard %
    "a_b"       // LIKE wildcard _
  )

  test("string literals are handled per dialect") {
    assertEquals(Dialect.postgresql.given_Dialect.quoteStringLiteral("a\nb", insideLikePattern=false), "E'a\\nb'");
    assertEquals(Dialect.sqlite.given_Dialect.quoteStringLiteral("a\nb", insideLikePattern=false), "'a\nb'");
  }

  private def testLikePatterns(conn: Connection)(using dialect: Dialect) = {
    val stmt = conn.createStatement()

    val underscorePattern = dialect.quoteStringLiteral("a_c", insideLikePattern=true)
    val literalUnderscorePattern = dialect.quoteStringLiteral(s"a${Dialect.literal_underscore}c", insideLikePattern=true)
    val percentPattern = dialect.quoteStringLiteral("a%c", insideLikePattern=true)
    val literalPercentPattern = dialect.quoteStringLiteral(s"a${Dialect.literal_percent}c", insideLikePattern=true)

    def check(query: String, expectedResult: Boolean) = {
      val rs = stmt.executeQuery(query + " as did_match")
      assert(rs.next())
      assertEquals(rs.getBoolean("did_match"), expectedResult)
    }

    check(s"SELECT 'abc' LIKE $underscorePattern", true);
    check(s"SELECT 'a_c' LIKE $underscorePattern", true);
    check(s"SELECT 'abc' LIKE $literalUnderscorePattern", false);
    check(s"SELECT 'a_c' LIKE $literalUnderscorePattern", true);

    check(s"SELECT 'abc' LIKE $percentPattern", true);
    check(s"SELECT 'a%c' LIKE $percentPattern", true);
    check(s"SELECT 'abc' LIKE $literalPercentPattern", false);
    check(s"SELECT 'a%c' LIKE $literalPercentPattern", true);
  }

  test("PostgreSQL LIKE patterns".tag(expensiveTest)) {
    withDB.postgres( { conn =>
      testLikePatterns(conn)
    })
  }

  test("MySQL LIKE patterns".tag(expensiveTest)) {
    withDB.mysql { conn =>
      testLikePatterns(conn)
    }
  }

  test("MariaDB LIKE patterns".tag(expensiveTest)) {
    withDB.mariadb { conn =>
      testLikePatterns(conn)
    }
  }

  test("SQLite LIKE patterns".tag(expensiveTest)) {
    withDB.sqlite { conn =>
      testLikePatterns(conn)
    }
  }

  test("H2 LIKE patterns".tag(expensiveTest)) {
    withDB.h2 { conn =>
      testLikePatterns(conn)
    }
  }

  test("DuckDB LIKE patterns".tag(expensiveTest)) {
    withDB.duckdb { conn =>
      testLikePatterns(conn)
    }
  }

  test("PostgreSQL string literals".tag(expensiveTest)) {
    withDB.postgres { conn =>
      interestingStrings.foreach(input => testStringLiteral(conn, input))
    }
  }

  test("MySQL and MariaDB string literals".tag(expensiveTest)) {
    withDB.mysql { conn =>
      interestingStrings.foreach(input => testStringLiteral(conn, input))
    }
    withDB.mariadb { conn =>
      interestingStrings.foreach(input => testStringLiteral(conn, input))
    }
  }

  test("SQLite string literals".tag(expensiveTest)) {
    withDB.sqlite { conn =>
      interestingStrings.foreach(input => testStringLiteral(conn, input))
    }
  }

  test("H2 string literals".tag(expensiveTest)) {
    withDB.h2 { conn =>
      interestingStrings.foreach(input => testStringLiteral(conn, input))
    }
  }

  test("DuckDB string literals".tag(expensiveTest)) {
    withDB.duckdb { conn =>
      interestingStrings.foreach(input => testStringLiteral(conn, input))
    }
  }
}
