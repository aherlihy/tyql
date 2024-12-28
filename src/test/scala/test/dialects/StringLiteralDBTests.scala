package test.integration.stringliteralescaping

import munit.FunSuite
import test.needsDBs
import tyql.Dialect
import test.withDB
import java.sql.{Connection, DriverManager}

class StringLiteralDBTests extends FunSuite {
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
    "a\bb", // Backspace
    "a\fb",
    "a\nb",
    "a\rb",
    "a\tb",
    "a\u001Ab", // Ctrl+Z
    "a%b",      // LIKE wildcard %
    "a_b",      // LIKE wildcard _
    "éèêëàâäôöûüùïîçæœ ÉÈÊËÀÂÄÔÖÛÜÙÏÎÇÆŒ", // French
    "äöüßÄÖÜ",                             // German
    "ąćęłńóśźżĄĆĘŁŃÓŚŹŻ",                  // Polish
    "абвгдеёжзийклмнопрстуфхцчшщъыьэюяАБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЪЫЬЭЮЯ", // Russian
    "αβγδεζηθικλμνξοπρστυφχψωΑΒΓΔΕΖΗΘΙΚΛΜΝΞΟΠΡΣΤΥΦΧΨΩάέήίόύώΆΈΉΊΌΎΏ",     // Greek
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

  test("all DBs handle LIKE patterns".tag(needsDBs)) {
    withDB.all { conn =>
      testLikePatterns(conn)
    }
  }

  test("all DBs handle string literals".tag(needsDBs)) {
    withDB.all { conn =>
      interestingStrings.foreach(input => testStringLiteral(conn, input))
    }
  }
}
