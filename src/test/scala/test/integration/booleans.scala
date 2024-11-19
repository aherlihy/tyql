package test.integration.booleans

import munit.FunSuite
import test.{withDBNoImplicits, withDB, checkExpr, checkExprDialect}
import java.sql.{Connection, Statement, ResultSet}
import tyql.{Dialect, Table, Expr}

class BooleanTests extends FunSuite {
  val t = Expr.BooleanLit(true)
  val f = Expr.BooleanLit(false)
  def expect(expected: Boolean)(rs: ResultSet) = assertEquals(rs.getBoolean(1), expected)

  test("boolean encoding") {
    checkExprDialect[Boolean](t, expect(true))(withDB.all)
    checkExprDialect[Boolean](f, expect(false))(withDB.all)
  }

  test("OR table") {
    checkExprDialect[Boolean](f || f, expect(false))(withDB.all)
    checkExprDialect[Boolean](f || t, expect(true))(withDB.all)
    checkExprDialect[Boolean](t || f, expect(true))(withDB.all)
    checkExprDialect[Boolean](t || t, expect(true))(withDB.all)
  }

  test("AND table") {
    checkExprDialect[Boolean](f && f, expect(false))(withDB.all)
    checkExprDialect[Boolean](f && t, expect(false))(withDB.all)
    checkExprDialect[Boolean](t && f, expect(false))(withDB.all)
    checkExprDialect[Boolean](t && t, expect(true))(withDB.all)
  }

  test("NOT table") {
    checkExprDialect[Boolean](!f, expect(true))(withDB.all)
    checkExprDialect[Boolean](!t, expect(false))(withDB.all)
  }

  test("XOR table") {
    checkExprDialect[Boolean](f ^ f, expect(false))(withDB.all)
    checkExprDialect[Boolean](f ^ t, expect(true))(withDB.all)
    checkExprDialect[Boolean](t ^ f, expect(true))(withDB.all)
    checkExprDialect[Boolean](t ^ t, expect(false))(withDB.all)
  }

  // TODO currently very broken!
  test("precedence".ignore) {
    checkExprDialect[Boolean](t || (f && f), expect(true))(withDB.all)
    checkExprDialect[Boolean]((t || f) && f, expect(false))(withDB.all)
  }
}
