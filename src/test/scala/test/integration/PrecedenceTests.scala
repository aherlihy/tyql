package test.integration.precedence

import munit.FunSuite
import test.{withDB, checkExprDialect}
import java.sql.ResultSet
import tyql.Expr
import scala.language.implicitConversions

import tyql.Dialect.ansi.given

class PrecedenceTests extends FunSuite {
  def expectB(expected: Boolean)(rs: ResultSet) = assertEquals(rs.getBoolean(1), expected)
  def expectI(expected: Int)(rs: ResultSet) = assertEquals(rs.getInt(1), expected)
  def expectD(expected: Double)(rs: ResultSet) = assertEquals(rs.getDouble(1), expected)

  val t = tyql.True
  val f = tyql.False
  val i1 = tyql.lit(1)
  val i2 = tyql.lit(2)
  val i3 = tyql.lit(3)

  test("boolean precedence -- AND over OR") {
    checkExprDialect[Boolean](t || f && f, expectB(true))(withDB.all)
    checkExprDialect[Boolean](t && f || t, expectB(true))(withDB.all)
  }

  test("boolean precedence -- NOT over AND/OR") {
    checkExprDialect[Boolean](!f && t, expectB(true))(withDB.all)
    checkExprDialect[Boolean](!t || f, expectB(false))(withDB.all)
    checkExprDialect[Boolean](!t && !f, expectB(false))(withDB.all)
  }

  test("integer precedence -- multiplication over addition") {
    checkExprDialect[Int](i1 + i2 * i3, expectI(7))(withDB.all)
    checkExprDialect[Int](i1 * i2 + i3, expectI(5))(withDB.all)
    checkExprDialect[Int](i1 * i2 * i3, expectI(6))(withDB.all)
  }

  test("integer precedence -- comparison with arithmetic") {
    checkExprDialect[Boolean](i1 + i2 > i3, expectB(false))(withDB.all)
    checkExprDialect[Boolean](i1 < i2 + i3, expectB(true))(withDB.all)
    checkExprDialect[Boolean](i1 * i2 == i3, expectB(false))(withDB.all)
  }

  test("mixed precedence -- comparison with boolean ops") {
    checkExprDialect[Boolean](i1 < i2 && i2 < i3, expectB(true))(withDB.all)
    checkExprDialect[Boolean](i1 < i2 || i3 < i2, expectB(true))(withDB.all)
    checkExprDialect[Boolean](i1 + i2 > i3 && t, expectB(false))(withDB.all)
  }

  test("boolean precedence -- multiple AND/OR") {
    checkExprDialect[Boolean](t || f && f || t, expectB(true))(withDB.all)
    checkExprDialect[Boolean](t && f || t && f, expectB(false))(withDB.all)
    checkExprDialect[Boolean](t && (f || t) && f, expectB(false))(withDB.all)
  }

  test("integer precedence -- multiple operations") {
    checkExprDialect[Int](i1 + i2 * i3 - i1, expectI(6))(withDB.all)
    checkExprDialect[Int](i1 * (i2 + i3) - i1, expectI(4))(withDB.all)
    checkExprDialect[Int](i1 * i2 + i3 * i1, expectI(5))(withDB.all)
  }

  test("mixed precedence -- complex expressions") {
    checkExprDialect[Boolean]((i1 + i2) * i3 > i1 && t, expectB(true))(withDB.all)
    checkExprDialect[Boolean](i1 < i2 + i3 && i2 * i3 > i1, expectB(true))(withDB.all)
    checkExprDialect[Boolean](i1 + i2 * i3 == i3 + i1 && t, expectB(false))(withDB.all)
  }

  test("boolean precedence -- XOR over AND/OR") {
    checkExprDialect[Boolean](t ^ f && t, expectB(true))(withDB.all)
    checkExprDialect[Boolean](t && f ^ t, expectB(true))(withDB.all)
    checkExprDialect[Boolean](t ^ t || f, expectB(false))(withDB.all)
  }

  test("integer precedence -- division and subtraction") {
    checkExprDialect[Double](i3 / i1 - i2.asDouble, expectD(1.0))(withDB.all)
    checkExprDialect[Double](i3.asDouble - i1 / i2, expectD(2.5))(withDB.all)
    checkExprDialect[Double](i3 / (i1 - i2), expectD(-3.0))(withDB.all)
  }

  test("mixed precedence -- arithmetic with boolean") {
    checkExprDialect[Boolean](i1 + i2 > i3 || t, expectB(true))(withDB.all)
    checkExprDialect[Boolean](i1 * i2 < i3 && f, expectB(false))(withDB.all)
    checkExprDialect[Boolean](i1 - i2 == i3 || t, expectB(true))(withDB.all)
  }
}
