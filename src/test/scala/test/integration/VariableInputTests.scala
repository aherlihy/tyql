package test.integration.variabelinupts

import munit.FunSuite
import test.{withDBNoImplicits, withDB, checkExpr, checkExprDialect}
import java.sql.{Connection, Statement, ResultSet}
import tyql._
import scala.language.implicitConversions

class VariableInputTest extends FunSuite {
  test("escaped in line works normally") {
    withDB.all { conn =>
      given tyql.Config = new tyql.Config(tyql.CaseConvention.Underscores, tyql.ParameterStyle.EscapedInline) {}

      def doIt(): Int =
        10

      val db = tyql.DB(conn)
      val q = Exprs[(a: Int, b: Long, c: String)](Var(doIt()), Var(2L), Var("c"))
      for (outer <- List(1, 2, 3)) {
        assert(q.toQueryIR.toSQLQuery()._1.contains("as a, 2 as b, 'c' as c"))
        val got = db.run(q)
        assertEquals(got.length, 1)
        assertEquals(
          got(0).toList,
          List(10, 2L, "c")
        )
      }
    }
  }

  test("driver-parametrized works as intended") {
    def doIt(): Int =
      10
    given tyql.Config = new tyql.Config(tyql.CaseConvention.Exact, tyql.ParameterStyle.DriverParametrized) {}
    val q = Exprs[(a: Int, b: Long, c: String)](Var(doIt()), Var(2L), Var("c"))

    // XXX why does it not work with H2?
    withDB.allWithoutH2 { conn =>
      val db = tyql.DB(conn)
      for (outer <- List(1, 2, 3)) {
        val sqlOutputted = q.toQueryIR.toSQLQuery()._1
        assert(
          (sqlOutputted.contains("$1 as a, $2 as b, $3 as c")) || (sqlOutputted.contains("? as a, ? as b, ? as c"))
        )
        val got = db.run(q)
        assertEquals(got.length, 1)
        assertEquals(
          got(0).toList,
          List(10, 2L, "c")
        )
      }
    }
  }

  test("caching works for one dialect, one config") {
    var state = 10
    def doIt(): Int =
      state += 1
      state

    import tyql.Dialect.postgresql.given
    given tyql.Config = new tyql.Config(tyql.CaseConvention.Exact, tyql.ParameterStyle.DriverParametrized) {}
    val q = Exprs[(a: Int, b: Long, c: String)](Var(doIt()), Var(2L), Var("c"))

    withDB.postgres { conn =>
      val db = tyql.DB(conn)
      for (outer <- List(1, 2, 3)) {
        val sqlOutputted = q.toQueryIR.toSQLQuery()._1
        assert(
          (sqlOutputted.contains("$1 as a, $2 as b, $3 as c")) || (sqlOutputted.contains("? as a, ? as b, ? as c"))
        )
        val got = db.run(q)
        assertEquals(got.length, 1)
        assertEquals(
          got(0).toList,
          List(10 + outer, 2L, "c")
        )
      }
    }
  }
}
