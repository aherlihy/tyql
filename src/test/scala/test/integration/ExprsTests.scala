package test.integration.exprs

import munit.FunSuite
import test.{withDBNoImplicits, withDB, checkExpr, checkExprDialect}
import java.sql.{Connection, Statement, ResultSet}
import tyql._
import tyql.Dialect.mysql.given

class ExprsTest extends FunSuite {
  test("exprs with literals") {
    withDB.all { conn =>
      val db = tyql.DB(conn)
      val q = Exprs[(a: Long, b: String)]((12L, "hello"))
      val got = db.run(q)
      assertEquals(got, List((12L, "hello")))
    }
  }

  test("exprs with exprs") {
    withDB.all { conn =>
      val db = tyql.DB(conn)
      val q = Exprs[(a: Long, b: String)]((12L, lit("hello") + lit(" world!")))
      val got = db.run(q)
      assertEquals(got, List((12L, "hello world!")))
    }
  }
}
