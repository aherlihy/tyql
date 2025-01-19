package test.integration

import munit.FunSuite
import tyql.*
import test.{withDB, checkExprDialect}
import java.sql.ResultSet
import scalasql.query.Nulls

private case class R(i: Int)
private val t = Table[R]("t")

class NullTests extends FunSuite {
  def expectB(expected: Boolean)(rs: ResultSet) = assertEquals(rs.getBoolean(1), expected)
  def expectI(expected: Int)(rs: ResultSet) = assertEquals(rs.getInt(1), expected)
  def expectS(expected: String)(rs: ResultSet) = assertEquals(rs.getString(1), expected)
  def expectN(expected: String | scala.Null)(rs: ResultSet) = assertEquals(rs.getString(1), expected)

  test("NULL use compiles at all despite no type tag provided") {
    t.map(_ => Null).toQueryIR.toSQLString()
  }

  test("isNull") {
    checkExprDialect[Boolean](Null.isNull, expectB(true))(withDB.all)
    checkExprDialect[Boolean](lit("hello").isNull, expectB(false))(withDB.all)
    checkExprDialect[Boolean](lit(101).isNull, expectB(false))(withDB.all)
  }

  test("!isNull is simplified") {
    checkExprDialect[Boolean](!Null.isNull, expectB(false), s => assert(s.toLowerCase().contains("is not null")))(
      withDB.all
    )
    checkExprDialect[Boolean](
      !lit("hello").isNull,
      expectB(true),
      s => assert(s.toLowerCase().contains("is not null"))
    )(withDB.all)
    checkExprDialect[Boolean](!lit(101).isNull, expectB(true), s => assert(s.toLowerCase().contains("is not null")))(
      withDB.all
    )
  }

  test("coalesce compiles") {
    // XXX the first Null you must type, but the second is inferred
    import scala.language.implicitConversions
    t.map(_ => Expr.coalesce(1, Null))
    t.map(_ => Expr.coalesce(Null[Int], 1))
    t.map(_ => Expr.coalesce("aaa", Null))
    t.map(_ => Expr.coalesce(Null[String], "aaa"))
    t.map(_ => Expr.coalesce(Null[String], "aaa", Null))
  }

  test("coalesce") {
    import scala.language.implicitConversions
    checkExprDialect[Int](Expr.coalesce(Null, 1), expectI(1))(withDB.all)
    checkExprDialect[Int](Expr.coalesce(1, Null), expectI(1))(withDB.all)
    checkExprDialect[String](Expr.coalesce("aaa", Null), expectS("aaa"))(withDB.all)
    checkExprDialect[String](Expr.coalesce(Null, "aaa"), expectS("aaa"))(withDB.all)
    checkExprDialect[String](Expr.coalesce(Null, "aaa", "bbb"), expectS("aaa"))(withDB.all)
    checkExprDialect[String](Expr.coalesce("aaa", "bbb", "ccc"), expectS("aaa"))(withDB.all)
  }

  test("nullIf") {
    import scala.language.implicitConversions
    checkExprDialect[String](Expr.nullIf("a", "b"), expectN("a"))(withDB.all)
    checkExprDialect[String](Expr.nullIf("a", "a"), expectN(null))(withDB.all)
    checkExprDialect[String](Expr.nullIf("a", Null), expectN("a"))(withDB.all)
    checkExprDialect[String](Expr.nullIf(Null[String], "b"), expectN(null))(withDB.all)
    checkExprDialect[String](Expr.nullIf(Null[String], Null), expectN(null))(withDB.all)
  }

  test("nullIf alternative syntax") {
    import scala.language.implicitConversions
    assertEquals(Expr.nullIf("a", "b"), lit("a").nullIf("b"))
    assertEquals(Expr.nullIf(Null[String], "b"), Null[String].nullIf("b"))
    assertEquals(Expr.nullIf("a", Null), lit("a").nullIf(Null))
    assertEquals(Expr.nullIf(Null[String], Null), Null[String].nullIf(Null))
  }
}
