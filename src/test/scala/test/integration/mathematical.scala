package test.integration

import munit.FunSuite
import java.sql.ResultSet
import tyql.*
import test.{checkExpr, withDBNoImplicits}
import scala.language.implicitConversions

class MathematicalOperationsTest extends FunSuite {
  def expectI(expected: Int)(rs: ResultSet) = assertEquals(rs.getInt(1), expected)
  def expectD(expected: Double)(rs: ResultSet) =
    val d = rs.getDouble(1)
    assert(Math.abs(d - expected) < 0.0001, s"Expected $expected, got $d")

  test("modulo") {
    checkExpr[Int](lit(10) % 3, expectI(1))(withDBNoImplicits.all)
    checkExpr[Int](lit(-5) % 3, expectI(-2))(withDBNoImplicits.all)
    checkExpr[Int](lit(-6) % 3, expectI(0))(withDBNoImplicits.all)
    checkExpr[Int](lit(-4) % 3, expectI(-1))(withDBNoImplicits.all)
  }

  test("rounding") {
    checkExpr[Int](lit(0.5).round, expectI(1))(withDBNoImplicits.all)
    checkExpr[Int](lit(0.49999).round, expectI(0))(withDBNoImplicits.all)
    checkExpr[Int](lit(-0.49999).round, expectI(0))(withDBNoImplicits.all)
    checkExpr[Int](lit(-0.5).round, expectI(-1))(withDBNoImplicits.all)

    checkExpr[Double](lit(0.1264).round(0), expectD(0))(withDBNoImplicits.all)
    checkExpr[Double](lit(0.1264).round(1), expectD(0.1))(withDBNoImplicits.all)
    checkExpr[Double](lit(0.1264).round(2), expectD(0.13))(withDBNoImplicits.all)
    checkExpr[Double](lit(0.1264).round(3), expectD(0.126))(withDBNoImplicits.all)
  }

  test("ceil, floor") {
    checkExpr[Int](lit(0.5).ceil, expectI(1))(withDBNoImplicits.all)
    checkExpr[Int](lit(0.5).floor, expectI(0))(withDBNoImplicits.all)
    checkExpr[Int](lit(-0.5).ceil, expectI(0))(withDBNoImplicits.all)
    checkExpr[Int](lit(-0.5).floor, expectI(-1))(withDBNoImplicits.all)
    checkExpr[Int](lit(.0).floor, expectI(0))(withDBNoImplicits.all)
    checkExpr[Int](lit(.0).ceil, expectI(0))(withDBNoImplicits.all)
  }

  test("power, sqrt") {
    checkExpr[Double](lit(10).power(3.0), expectD(1000.0))(withDBNoImplicits.all)
    checkExpr[Double](lit(100).power(.5), expectD(10.0))(withDBNoImplicits.all)
    checkExpr[Double](lit(100).power(.0), expectD(1.0))(withDBNoImplicits.all)

    checkExpr[Double](lit(100).sqrt, expectD(10.0))(withDBNoImplicits.all)
    checkExpr[Double](lit(1).sqrt, expectD(1.0))(withDBNoImplicits.all)
    checkExpr[Double](lit(0).sqrt, expectD(0.0))(withDBNoImplicits.all)
  }

  test("sign") {
    checkExpr[Int](lit(10).sign, expectI(1))(withDBNoImplicits.all)
    checkExpr[Int](lit(10.4).sign, expectI(1))(withDBNoImplicits.all)
    checkExpr[Int](lit(-10.5).sign, expectI(-1))(withDBNoImplicits.all)
    checkExpr[Int](lit(-10).sign, expectI(-1))(withDBNoImplicits.all)
    checkExpr[Int](lit(0).sign, expectI(0))(withDBNoImplicits.all)
    checkExpr[Int](lit(0.0).sign, expectI(0))(withDBNoImplicits.all)
  }

  test("logarithms") {
    checkExpr[Double](lit(10).ln, expectD(2.302585092994046))(withDBNoImplicits.all)
    checkExpr[Double](lit(10).log10, expectD(1.0))(withDBNoImplicits.all)
    checkExpr[Double](lit(1024).log2, expectD(10))(withDBNoImplicits.all)
    checkExpr[Double](tyql.Expr.exp(13).ln, expectD(13))(withDBNoImplicits.all)
  }

  test("trigonometric functions") {
    checkExpr[Double](Expr.sin(100).power(2.0) + Expr.cos(100) * Expr.cos(100), expectD(1.0))(withDBNoImplicits.all)
    checkExpr[Double](Expr.sin(Expr.asin(0.76)), expectD(0.76))(withDBNoImplicits.all)
    checkExpr[Double](Expr.cos(Expr.acos(0.76)), expectD(0.76))(withDBNoImplicits.all)
    checkExpr[Double](Expr.tan(Expr.atan(0.76)), expectD(0.76))(withDBNoImplicits.all)
    checkExpr[Double](Expr.cos(17).power(2.0) - Expr.sin(17).power(2.0), expectD(Math.cos(17*2)))(withDBNoImplicits.all)
  }
}
