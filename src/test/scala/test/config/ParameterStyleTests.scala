package test.config.parameterstyletests

import munit.FunSuite
import tyql.{Config, Dialect}
import tyql.{CaseConvention, ParameterStyle}
import test.withDB
import test.needsDBs

import tyql.{Table, DatabaseAST, Query}
import scala.language.experimental.namedTuples
import NamedTuple.{NamedTuple, AnyNamedTuple}
import com.mysql.cj.x.protobuf.MysqlxNotice.SessionStateChanged.Parameter
import org.junit.experimental.theories.ParameterSupplier

class ParameterStyleTests extends FunSuite {

  case class Flowers(name: Option[String], flowerSize: Int, cost: Option[Double], likes: Int)
  val t = tyql.Table[Flowers]()

  test("by default escaped (will be changed before public release)") {
    val q = t.filter(_.name == "Lily")
    val (sql, parameters) = q.toQueryIR.toSQLQuery()
    assertEquals(parameters.length, 0)
    assert(sql.contains("'Lily'"))
    assert(!sql.contains("?"))
  }

  test("configured to EscapeInline") {
    given tyql.Config = new tyql.Config(CaseConvention.Exact, ParameterStyle.EscapedInline) {}

    val q = t.filter(_.name == "Lily")
    val (sql, parameters) = q.toQueryIR.toSQLQuery()
    assertEquals(parameters.length, 0)
    assert(sql.contains("'Lily'"))
    assert(!sql.contains("?"))
  }

  test("configured to DriverParametrized") {
    given tyql.Config = new tyql.Config(CaseConvention.Exact, ParameterStyle.DriverParametrized) {}

    val q = t.filter(_.name == "Lily")
    val (sql, parameters) = q.toQueryIR.toSQLQuery()
    assertEquals(parameters.length, 1)
    assertEquals(parameters(0).toString(), "Lily")
    assert(!sql.contains("'Lily'"))
    assert(sql.contains("?"))
  }
}
