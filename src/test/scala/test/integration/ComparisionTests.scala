package test.integration.comparions

import munit.FunSuite
import test.{withDBNoImplicits, withDB, checkExpr, checkExprDialect}
import java.sql.{Connection, Statement, ResultSet}
import tyql.{Dialect, Table, Expr}
import tyql.lit

class ComparisonTests extends FunSuite {
  def expect(expected: Boolean)(rs: ResultSet) = assertEquals(rs.getBoolean(1), expected)

  test("you can compare different numerics") {
    checkExprDialect[Boolean](lit(1) < lit(20.0), expect(true))(withDB.all)
    checkExprDialect[Boolean](lit(10.23) < lit(-3), expect(false))(withDB.all)
  }

  test("you can compare strings") {
    checkExprDialect[Boolean](lit("a") < lit("b"), expect(true))(withDB.all)
    checkExprDialect[Boolean](lit("zbdfjsahfkjsd") < lit("dfsdfa"), expect(false))(withDB.all)
  }

  test("you cannot compare different types int string") {
    val error: String =
      compileErrors("""
      import tyql._
      import tyql.Expr._

      lit(1) < lit("a")
      """)
    assert(error.contains("No implicit Ordering defined for String"), s"expected to fail, but got <<<${error}>>>")
  }

  test("you cannot compare different types string int") {
    val error: String =
      compileErrors("""
      import tyql._
      import tyql.Expr._

      lit("a") < lit(1)
      """)
    assert(error.contains("Required: String"), s"expected to fail, but got <<<${error}>>>")
  }
}
