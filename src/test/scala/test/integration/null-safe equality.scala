package test.integration

import munit.FunSuite
import java.sql.ResultSet
import tyql.*
import test.{checkExpr, checkExprDialect, withDBNoImplicits, withDB}
import scala.language.implicitConversions

class NullSafeEqualityTest extends FunSuite {
  def expectN(expected: String | scala.Null)(rs: ResultSet) = assertEquals(rs.getString(1), expected)
  def expectB(expected: Boolean)(rs: ResultSet) = assertEquals(rs.getBoolean(1), expected)

  test("non-null-safe equality") {
    checkExpr[Boolean](lit("a") == "a", expectB(true))(withDBNoImplicits.all)
    checkExpr[Boolean](lit("a") == "b", expectB(false))(withDBNoImplicits.all)
    // checkExpr[Boolean](lit("a") == 10, expectB(false), println)(withDBNoImplicits.all)
    /*
      TODO:
      Postgres and DuckDB do not allow you to compare things that are not of the same type affinity at all.
      Currently we implement universal equality for == (which will break at runtime for Postgres and DuckDB),
      for the null-safe equality we only allow the same types (which is too restrictive).
      What to do about this?
     */
    checkExpr[Boolean](lit("a") == tyql.Null, expectN(null))(withDBNoImplicits.all)
    checkExpr[Boolean](tyql.Null == lit("a"), expectN(null))(withDBNoImplicits.all)
    checkExpr[Boolean](tyql.Null == tyql.Null, expectN(null))(withDBNoImplicits.all)
  }

  test("null-safe equality") {
    checkExprDialect[Boolean](lit("a") === "a", expectB(true))(withDB.all)
    checkExprDialect[Boolean](lit("a") === "b", expectB(false))(withDB.all)
    checkExprDialect[Boolean](lit("a") === tyql.Null[String], expectB(false))(withDB.all)
    checkExprDialect[Boolean](tyql.Null[String] === lit("a"), expectB(false))(withDB.all)
    checkExprDialect[Boolean](tyql.Null === tyql.Null, expectB(true))(withDB.all)
  }
}
