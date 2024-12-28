package test.integration

import munit.FunSuite
import tyql.*
import test.{checkExprDialect, withDB}
import scala.language.implicitConversions
import java.sql.ResultSet

class CastTests extends FunSuite {
  def expectI(expected: Int)(rs: ResultSet) = assertEquals(rs.getInt(1), expected)
  def expectD(expected: Double)(rs: ResultSet) = assertEquals(rs.getDouble(1), expected)
  def expectS(expected: String)(rs: ResultSet) = assertEquals(rs.getString(1), expected)
  def expectB(expected: Boolean)(rs: ResultSet) = assertEquals(rs.getBoolean(1), expected)

  test("parsing integers") {
    checkExprDialect[Int](lit("101").as[Int], expectI(101))(withDB.all)
    checkExprDialect[Int](lit("-200").as[Int], expectI(-200))(withDB.all)
    checkExprDialect[Int](lit("-0").as[Int], expectI(0))(withDB.all)
    checkExprDialect[Int](lit("0").as[Int], expectI(0))(withDB.all)
  }

  test("parsing doubles") {
    checkExprDialect[Double](lit("101.6").as[Double], expectD(101.6))(withDB.all)
    checkExprDialect[Double](lit("-601.6").as[Double], expectD(-601.6))(withDB.all)
    checkExprDialect[Double](lit("-0.0").as[Double], expectD(-0.0))(withDB.all)
    checkExprDialect[Double](lit("0.001").as[Double], expectD(0.001))(withDB.all)
  }

  test("toString") {
    checkExprDialect[String](lit(1056).as[String], expectS("1056"))(withDB.all)
    checkExprDialect[String](lit(-123.123).as[String], expectS("-123.123"))(withDB.all)
    checkExprDialect[String](lit("???Aałajć").as[String], expectS("???Aałajć"))(withDB.all)
  }

  test("booleans are interpreted as numbers") {
    checkExprDialect[Int](lit(true).as[Int], expectI(1))(withDB.all)
    checkExprDialect[Int](lit(false).as[Int], expectI(0))(withDB.all)
  }
}
