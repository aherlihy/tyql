package test.integration.stringliteralescaping

import munit.FunSuite
import test.expensiveTest
import tyql.Dialect
import java.sql.{Connection, DriverManager}

class StringLiteralDBTest extends FunSuite {
  def withConnection[A](url: String, user: String = "", password: String = "")(f: Connection => A): A = {
    var conn: Connection = null
    try {
      conn = DriverManager.getConnection(url, user, password)
      f(conn)
    } finally {
      if (conn != null) conn.close()
    }
  }

  private def testStringLiteral(dialect: Dialect, conn: Connection, input: String) = {
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

  private def testLikePatterns(dialect: Dialect, conn: Connection) = {
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
    withConnection("jdbc:postgresql://localhost:5433/testdb", "testuser", "testpass") { conn =>
      testLikePatterns(Dialect.postgresql.given_Dialect, conn)
    }
  }

  test("MySQL LIKE patterns".tag(expensiveTest)) {
    withConnection("jdbc:mysql://localhost:3307/testdb", "testuser", "testpass") { conn =>
      testLikePatterns(Dialect.mysql.given_Dialect, conn)
    }
  }

  test("MariaDB LIKE patterns".tag(expensiveTest)) {
    withConnection("jdbc:mariadb://localhost:3308/testdb", "testuser", "testpass") { conn =>
      testLikePatterns(Dialect.mariadb.given_Dialect, conn)
    }
  }

  test("SQLite LIKE patterns".tag(expensiveTest)) {
    withConnection("jdbc:sqlite::memory:") { conn =>
      testLikePatterns(Dialect.sqlite.given_Dialect, conn)
    }
  }

  test("H2 LIKE patterns".tag(expensiveTest)) {
    withConnection("jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1") { conn =>
      testLikePatterns(Dialect.h2.given_Dialect, conn)
    }
  }

  test("DuckDB LIKE patterns".tag(expensiveTest)) {
    withConnection("jdbc:duckdb:") { conn =>
      testLikePatterns(Dialect.duckdb.given_Dialect, conn)
    }
  }

  test("PostgreSQL string literals".tag(expensiveTest)) {
    withConnection("jdbc:postgresql://localhost:5433/testdb", "testuser", "testpass") { conn =>
      val dialect = Dialect.postgresql.given_Dialect
      interestingStrings.foreach(input => testStringLiteral(dialect, conn, input))
    }
  }

  test("MySQL and MariaDB string literals".tag(expensiveTest)) {
    withConnection("jdbc:mysql://localhost:3307/testdb", "testuser", "testpass") { conn =>
      val dialect = Dialect.mysql.given_Dialect
      interestingStrings.foreach(input => testStringLiteral(dialect, conn, input))
    }
    withConnection("jdbc:mariadb://localhost:3308/testdb", "testuser", "testpass") { conn =>
      val dialect = Dialect.mariadb.given_Dialect 
      interestingStrings.foreach(input => testStringLiteral(dialect, conn, input))
    }
  }

  test("SQLite string literals".tag(expensiveTest)) {
    withConnection("jdbc:sqlite::memory:") { conn =>
      val dialect = Dialect.sqlite.given_Dialect
      interestingStrings.foreach(input => testStringLiteral(dialect, conn, input))
    }
  }

  test("H2 string literals".tag(expensiveTest)) {
    withConnection("jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1") { conn =>
      val dialect = Dialect.h2.given_Dialect
      interestingStrings.foreach(input => testStringLiteral(dialect, conn, input))
    }
  }

  test("DuckDB string literals".tag(expensiveTest)) {
    withConnection("jdbc:duckdb:") { conn =>
      val dialect = Dialect.duckdb.given_Dialect
      interestingStrings.foreach(input => testStringLiteral(dialect, conn, input))
    }
  }
}
