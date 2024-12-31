package test.integration.cases

import munit.FunSuite
import test.{withDB, checkExprDialect}
import tyql.Expr.cases
import tyql.{lit, True, False, Else}
import java.sql.ResultSet
import tyql.NonScalarExpr

import tyql.Dialect.ansi.given

class CaseTests extends FunSuite {
  def expectI(expected: Int)(rs: ResultSet) = assertEquals(rs.getInt(1), expected)
  def expectB(expected: Boolean)(rs: ResultSet) = assertEquals(rs.getBoolean(1), expected)
  def expectS(expected: String)(rs: ResultSet) = assertEquals(rs.getString(1), expected)

  def usesElse(s: String) = assert(s.toLowerCase().contains(" else "))
  def noElse(s: String) = assert(!s.toLowerCase().contains(" else "))

  test("searched CASE") {
    checkExprDialect[Int](
      cases(
        True -> lit(10)
      ),
      expectI(10),
      noElse
    )(withDB.all)

    checkExprDialect[String](
      cases(
        (lit(20) < lit(10)) -> lit("lt"),
        (lit(20) == lit(10)) -> lit("eq"),
        (lit(20) > lit(10)) -> lit("gt")
      ),
      expectS("gt"),
      noElse
    )(withDB.all)

    checkExprDialect[String](
      cases(
        (lit(15) < lit(15)) -> lit("lt"),
        (lit(15) == lit(15)) -> lit("eq"),
        (lit(15) > lit(15)) -> lit("gt")
      ),
      expectS("eq"),
      noElse
    )(withDB.all)

    checkExprDialect[String](
      cases(
        (lit(15) < lit(100)) -> lit("lt"),
        (lit(15) == lit(100)) -> lit("eq"),
        (lit(15) > lit(100)) -> lit("gt")
      ),
      expectS("lt"),
      noElse
    )(withDB.all)

    checkExprDialect[String](
      cases(
        (lit(105) < lit(100)) -> lit("lt"),
        (lit(15) == lit(100)) -> lit("eq"),
        (lit(15) > lit(100)) -> lit("gt"),
        true -> lit("neither")
      ),
      expectS("neither"),
      usesElse
    )(withDB.all)

    checkExprDialect[Int](
      cases(
        False -> lit(10),
        true -> lit(20)
      ),
      expectI(20),
      usesElse
    )(withDB.all)

    checkExprDialect[Int](
      cases(
        False -> lit(10),
        Else -> lit(20)
      ),
      expectI(20),
      usesElse
    )(withDB.all)
  }

  test("simple CASE") {
    checkExprDialect[Int](
      lit(10).cases(
        lit(9) -> lit(90),
        lit(10) -> lit(100),
        lit(11) -> lit(110),
      ),
      expectI(100),
      noElse
    )(withDB.all)

    checkExprDialect[Boolean](
      lit("abba").cases(
        lit("aaaaa") -> False,
        lit("bbbfsfad") -> False,
        lit("abba") -> True,
        Else -> False
      ),
      expectB(true),
      usesElse
    )(withDB.all)
  }

  test("simple CASE does not allow LIKE patterns") {
    checkExprDialect[Boolean](
      lit("abba").cases(
        lit("a%") -> False,
        lit("%b%") -> False,
        lit("a__a") -> False,
        Else -> True
      ),
      expectB(true),
      usesElse
    )(withDB.all)
  }
}
