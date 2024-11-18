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

    val rs1 = stmt.executeQuery(s"SELECT 'abc' LIKE $underscorePattern as result")
    assert(rs1.next())
    assert(rs1.getBoolean("result"))
    val rs2 = stmt.executeQuery(s"SELECT 'a_c' LIKE $underscorePattern as result")
    assert(rs2.next())
    assert(rs2.getBoolean("result"))
    val rs3 = stmt.executeQuery(s"SELECT 'abc' LIKE $literalUnderscorePattern as result")
    assert(rs3.next())
    assert(!rs3.getBoolean("result"))
    val rs4 = stmt.executeQuery(s"SELECT 'a_c' LIKE $literalUnderscorePattern as result")
    assert(rs4.next())
    assert(rs4.getBoolean("result"), "the pattern was " + s"SELECT 'a_c' LIKE $literalUnderscorePattern as result")

    val percentPattern = dialect.quoteStringLiteral("a%c", insideLikePattern=true)
    val literalPercentPattern = dialect.quoteStringLiteral(s"a${Dialect.literal_percent}c", insideLikePattern=true)

    val rs5 = stmt.executeQuery(s"SELECT 'abc' LIKE $percentPattern as result")
    assert(rs5.next())
    assert(rs5.getBoolean("result"))
    val rs6 = stmt.executeQuery(s"SELECT 'a%c' LIKE $percentPattern as result")
    assert(rs6.next())
    assert(rs6.getBoolean("result"))
    val rs7 = stmt.executeQuery(s"SELECT 'abc' LIKE $literalPercentPattern as result")
    assert(rs7.next())
    assert(!rs7.getBoolean("result"))
    val rs8 = stmt.executeQuery(s"SELECT 'a%c' LIKE $literalPercentPattern as result")
    assert(rs8.next())
    assert(rs8.getBoolean("result"))
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
