package test.integration

import munit.FunSuite
import tyql.*
import test.{checkExprDialect, withDB}
import scala.language.implicitConversions
import java.sql.ResultSet

class CastTests extends FunSuite {
  def expectI(expected: Int)(rs: ResultSet) = assertEquals(rs.getInt(1), expected)
  def expectL(expected: Long)(rs: ResultSet) = assertEquals(rs.getLong(1), expected)
  def expectD(expected: Double)(rs: ResultSet) = assertEquals(rs.getDouble(1), expected)
  def expectF(expected: Float)(rs: ResultSet) = assertEquals(rs.getFloat(1), expected)
  def expectS(expected: String)(rs: ResultSet) = assertEquals(rs.getString(1), expected)
  def expectB(expected: Boolean)(rs: ResultSet) = assertEquals(rs.getBoolean(1), expected)

  test("parsing integers") {
    checkExprDialect[Int](lit("101").asInt, expectI(101))(withDB.all)
    checkExprDialect[Int](lit("-200").asInt, expectI(-200))(withDB.all)
    checkExprDialect[Int](lit("-0").asInt, expectI(0))(withDB.all)
    checkExprDialect[Int](lit("0").asInt, expectI(0))(withDB.all)

    checkExprDialect[Long](lit("101").asLong, expectL(101))(withDB.all)
    checkExprDialect[Long](lit("-200").asLong, expectL(-200))(withDB.all)
    checkExprDialect[Long](lit("-0").asLong, expectL(0))(withDB.all)
    checkExprDialect[Long](lit("0").asLong, expectL(0))(withDB.all)
  }

  test("parsing doubles") {
    checkExprDialect[Double](lit("101.6").asDouble, expectD(101.6))(withDB.all)
    checkExprDialect[Double](lit("-601.6").asDouble, expectD(-601.6))(withDB.all)
    checkExprDialect[Double](lit("-0.0").asDouble, expectD(-0.0))(withDB.all)
    checkExprDialect[Double](lit("0.001").asDouble, expectD(0.001))(withDB.all)

    checkExprDialect[Float](lit("101.6").asFloat, expectF(101.6))(withDB.all)
    checkExprDialect[Float](lit("-601.6").asFloat, expectF(-601.6))(withDB.all)
    checkExprDialect[Float](lit("-0.0").asFloat, expectF(-0.0))(withDB.all)
    checkExprDialect[Float](lit("0.001").asFloat, expectF(0.001))(withDB.all)
  }

  test("toString") {
    checkExprDialect[String](lit(1056).asString, expectS("1056"))(withDB.all)
    checkExprDialect[String](lit(-123.123).asString, expectS("-123.123"))(withDB.all)
    checkExprDialect[String](lit("???Aałajć").asString, expectS("???Aałajć"))(withDB.all)
  }

  test("booleans are interpreted as numbers") {
    checkExprDialect[Int](lit(true).asInt, expectI(1))(withDB.all)
    checkExprDialect[Int](lit(false).asInt, expectI(0))(withDB.all)
  }
}
