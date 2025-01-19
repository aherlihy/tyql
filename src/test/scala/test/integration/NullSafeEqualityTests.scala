package test.integration

import munit.FunSuite
import java.sql.ResultSet
import tyql.*
import test.{checkExpr, checkExprDialect, withDBNoImplicits, withDB}
import scala.language.implicitConversions

import tyql.Dialect.ansi.given

class NullSafeEqualityTests extends FunSuite {
  def expectN(expected: String | scala.Null)(rs: ResultSet) = assertEquals(rs.getString(1), expected)
  def expectB(expected: Boolean)(rs: ResultSet) = assertEquals(rs.getBoolean(1), expected)

  test("non-null-safe equality") {
    checkExpr[Boolean](lit("a") == "a", expectB(true))(withDBNoImplicits.all)
    checkExpr[Boolean](lit("a") == "b", expectB(false))(withDBNoImplicits.all)
    checkExpr[Boolean](lit("a") == tyql.Null[String], expectN(null))(withDBNoImplicits.all)
    checkExpr[Boolean](tyql.Null[String] == lit("a"), expectN(null))(withDBNoImplicits.all)
    checkExpr[Boolean](tyql.Null == tyql.Null, expectN(null))(withDBNoImplicits.all)
  }

  test("null-safe equality") {
    checkExprDialect[Boolean](lit("a") === "a", expectB(true))(withDB.all)
    checkExprDialect[Boolean](lit("a") === "b", expectB(false))(withDB.all)
    checkExprDialect[Boolean](lit("a") === tyql.Null[String], expectB(false))(withDB.all)
    checkExprDialect[Boolean](tyql.Null[String] === lit("a"), expectB(false))(withDB.all)
    checkExprDialect[Boolean](tyql.Null === tyql.Null, expectB(true))(withDB.all)
  }

  test("you can use weakly-typed === equality when not on postgres, duckdb, h2") {
    {
      import Dialect.mysql.given
      checkExprDialect[Boolean](lit("a") === 10, expectB(false))(withDB.allmysql)
      checkExprDialect[Boolean](lit("a") !== 10, expectB(true))(withDB.allmysql)
      checkExprDialect[Boolean](lit("a") == 10, expectB(false))(withDB.allmysql)
      checkExprDialect[Boolean](lit("a") != 10, expectB(true))(withDB.allmysql)
    }
    {
      import Dialect.sqlite.given
      checkExprDialect[Boolean](lit("a") === 10, expectB(false))(withDB.sqlite)
      checkExprDialect[Boolean](lit("a") !== 10, expectB(true))(withDB.sqlite)
      checkExprDialect[Boolean](lit("a") == 10, expectB(false))(withDB.sqlite)
      checkExprDialect[Boolean](lit("a") != 10, expectB(true))(withDB.sqlite)
    }
  }

  test("this still works with filters (postgresql)") {
    import Dialect.postgresql.given
    case class R(kiki: Int)
    val t = Table[R]("table")
    t.filter(p => p.kiki === 10)
    t.filter(p => p.kiki == 10)
    // t.filter(p => p.kiki !== "a")
    // t.filter(p => p.kiki != "a")
  }

  test("this still works with filters (mysql)") {
    import Dialect.mysql.given
    case class R(kiki: Int)
    val t = Table[R]("table")
    t.filter(p => p.kiki === 10)
    t.filter(p => p.kiki == 10)
    t.filter(p => p.kiki !== "a")
    t.filter(p => p.kiki != "a")
  }

  test("postgres/duckdb/h2 despite not having universal equality can still compare different numeric types") {
    {
      import Dialect.postgresql.given
      checkExprDialect[Boolean](lit(10) === 10.0, expectB(true))(withDB.postgres)
      checkExprDialect[Boolean](lit(10.0) === 10, expectB(true))(withDB.postgres)
    }
    {
      import Dialect.duckdb.given
      checkExprDialect[Boolean](lit(10) === 10.0, expectB(true))(withDB.duckdb)
      checkExprDialect[Boolean](lit(10.0) === 10, expectB(true))(withDB.duckdb)
    }
    {
      import Dialect.h2.given
      checkExprDialect[Boolean](lit(10) === 10.0, expectB(true))(withDB.h2)
      checkExprDialect[Boolean](lit(10.0) === 10, expectB(true))(withDB.h2)
    }
  }
}
