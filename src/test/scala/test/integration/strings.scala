package test.integration.strings

import munit.FunSuite
import test.{withDBNoImplicits, checkExpr}
import java.sql.{Connection, Statement, ResultSet}
import tyql.{Dialect, Table, Expr}
import tyql.Subset.a

class StringTests extends FunSuite {
  test("string length by characters and bytes, length is aliased to characterLength tests") {
    def checkValue(expected: Int)(rs: ResultSet) = assert(rs.getInt(1) == expected)

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
}
