package test.integration.strings

import munit.FunSuite
import test.{withDBNoImplicits, checkExpr}
import java.sql.{Connection, Statement, ResultSet}
import tyql.{Dialect, Table, Expr}
import tyql.Subset.a

class StringTests extends FunSuite {
  test("string length by characters and bytes, length is aliased to characterLength tests") {
    def checkValue(expected: Int)(rs: ResultSet) = assertEquals(rs.getInt(1), expected)

    {
      import Dialect.postgresql.given
      checkExpr[Int](Expr.StringLit("ałajć").length, checkValue(5))(withDBNoImplicits.postgres)
      checkExpr[Int](Expr.StringLit("ałajć").charLength, checkValue(5))(withDBNoImplicits.postgres)
      checkExpr[Int](Expr.StringLit("ałajć").byteLength, checkValue(7))(withDBNoImplicits.postgres)
    }
    {
      import Dialect.mysql.given
      checkExpr[Int](Expr.StringLit("ałajć").length, checkValue(5))(withDBNoImplicits.mysql)
      checkExpr[Int](Expr.StringLit("ałajć").charLength, checkValue(5))(withDBNoImplicits.mysql)
      checkExpr[Int](Expr.StringLit("ałajć").byteLength, checkValue(7))(withDBNoImplicits.mysql)
    }
    {
      import Dialect.mariadb.given
      checkExpr[Int](Expr.StringLit("ałajć").length, checkValue(5))(withDBNoImplicits.mariadb)
      checkExpr[Int](Expr.StringLit("ałajć").charLength, checkValue(5))(withDBNoImplicits.mariadb)
      checkExpr[Int](Expr.StringLit("ałajć").byteLength, checkValue(7))(withDBNoImplicits.mariadb)
    }
    {
      import Dialect.duckdb.given
      checkExpr[Int](Expr.StringLit("ałajć").length, checkValue(5))(withDBNoImplicits.duckdb)
      checkExpr[Int](Expr.StringLit("ałajć").charLength, checkValue(5))(withDBNoImplicits.duckdb)
      checkExpr[Int](Expr.StringLit("ałajć").byteLength, checkValue(7))(withDBNoImplicits.duckdb)
    }
    {
      import Dialect.h2.given
      checkExpr[Int](Expr.StringLit("ałajć").length, checkValue(5))(withDBNoImplicits.h2)
      checkExpr[Int](Expr.StringLit("ałajć").charLength, checkValue(5))(withDBNoImplicits.h2)
      checkExpr[Int](Expr.StringLit("ałajć").byteLength, checkValue(7))(withDBNoImplicits.h2)
    }
    {
      import Dialect.sqlite.given
      checkExpr[Int](Expr.StringLit("ałajć").length, checkValue(5))(withDBNoImplicits.sqlite)
      checkExpr[Int](Expr.StringLit("ałajć").charLength, checkValue(5))(withDBNoImplicits.sqlite)
      checkExpr[Int](Expr.StringLit("ałajć").byteLength, checkValue(7))(withDBNoImplicits.sqlite)
    }
  }

  test("upper and lower work also with unicode") {
    def checkValue(expected: String)(rs: ResultSet) = assertEquals(rs.getString(1), expected)

    for (r <- Seq(withDBNoImplicits.postgres, withDBNoImplicits.mariadb, withDBNoImplicits.mysql, withDBNoImplicits.h2, withDBNoImplicits.duckdb)) {
      checkExpr[String](Expr.StringLit("aŁaJć").toUpperCase, checkValue("AŁAJĆ"))(r.asInstanceOf[(java.sql.Connection => Unit) => Unit])
      checkExpr[String](Expr.StringLit("aŁaJć").toLowerCase, checkValue("ałajć"))(r.asInstanceOf[(java.sql.Connection => Unit) => Unit])
    }

    // SQLite does not support unicode case folding by default unless compiled with ICU support
    checkExpr[String](Expr.StringLit("A bRoWn fOX").toUpperCase, checkValue("A BROWN FOX"))(withDBNoImplicits.sqlite)
    checkExpr[String](Expr.StringLit("A bRoWn fOX").toLowerCase, checkValue("a brown fox"))(withDBNoImplicits.sqlite)
  }
}
