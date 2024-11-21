package test.integration.strings

import munit.FunSuite
import test.{withDBNoImplicits, withDB, checkExpr, checkExprDialect}
import java.sql.{Connection, Statement, ResultSet}
import tyql.{Dialect, Table, Expr}

class StringTests extends FunSuite {
  test("string length by characters and bytes, length is aliased to characterLength tests") {
    def checkValue(expected: Int)(rs: ResultSet) = assertEquals(rs.getInt(1), expected)

    // XXX the expression is defined under ANSI SQL dialect, but toSQLQuery is run against a specific dialect and it works!
    assertEquals(summon[Dialect].name(), "ANSI SQL Dialect")
    checkExprDialect[Int](tyql.lit("ałajć").length, checkValue(5))(withDB.all)
    checkExprDialect[Int](tyql.lit("ałajć").charLength, checkValue(5))(withDB.all)
    checkExprDialect[Int](tyql.lit("ałajć").byteLength, checkValue(7))(withDB.all)
  }

  test("upper and lower work also with unicode") {
    def checkValue(expected: String)(rs: ResultSet) = assertEquals(rs.getString(1), expected)

    for (r <- Seq(withDBNoImplicits.postgres[Unit], withDBNoImplicits.mariadb[Unit],
                  withDBNoImplicits.mysql[Unit], withDBNoImplicits.h2[Unit], withDBNoImplicits.duckdb[Unit])) {
      checkExpr[String](tyql.lit("aŁaJć").toUpperCase, checkValue("AŁAJĆ"))(r)
      checkExpr[String](tyql.lit("aŁaJć").toLowerCase, checkValue("ałajć"))(r)
    }

    // SQLite does not support unicode case folding by default unless compiled with ICU support
    checkExpr[String](tyql.lit("A bRoWn fOX").toUpperCase, checkValue("A BROWN FOX"))(withDBNoImplicits.sqlite)
    checkExpr[String](tyql.lit("A bRoWn fOX").toLowerCase, checkValue("a brown fox"))(withDBNoImplicits.sqlite)
  }
}
