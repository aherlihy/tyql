package test.integration.strings

import munit.FunSuite
import test.withDBNoImplicits
import java.sql.{Connection, Statement, ResultSet}
import tyql.{Dialect, Table, Expr}
import tyql.Subset.a

class StringTests extends FunSuite {
  def init(stmt: Statement) = {
    stmt.executeUpdate(s"DROP TABLE IF EXISTS table1733;")
    stmt.executeUpdate(s"CREATE TABLE table1733 (i INTEGER);")
    stmt.executeUpdate(s"INSERT INTO table1733 (i) VALUES (1);");
  }

  def clean(stmt: Statement) = {
    stmt.executeUpdate(s"DROP TABLE IF EXISTS table1733;")
  }

  case class Row(i: Int)
  val t = Table[Row]("table1733")

  def check(sqlQuery: String, checkValue: ResultSet => Unit)(runner: (f: Connection => Unit) => Unit): Unit = {
    for (_ <- 1 to 3) {
      runner { conn =>
        val stmt = conn.createStatement()
        init(stmt)
        val rs = stmt.executeQuery(sqlQuery)
        checkValue(rs)
        clean(stmt)
      }
    }
  }

  test("string length by characters and bytes, length is aliased to characterLength tests") {
    import scala.language.implicitConversions

    def checkValue(expected: Int)(rs: ResultSet) =
      assert(rs.next())
      val r = rs.getInt(1)
      assert(r == expected)

    {
      import Dialect.postgresql.given
      check(t.map(_ => Expr.StringLit("ałajć").length).toQueryIR.toSQLString(), checkValue(5))(withDBNoImplicits.postgres)
      check(t.map(_ => Expr.StringLit("ałajć").charLength).toQueryIR.toSQLString(), checkValue(5))(withDBNoImplicits.postgres)
      check(t.map(_ => Expr.StringLit("ałajć").byteLength).toQueryIR.toSQLString(), checkValue(7))(withDBNoImplicits.postgres)
    }
    {
      import Dialect.mysql.given
      check(t.map(_ => Expr.StringLit("ałajć").length).toQueryIR.toSQLString(), checkValue(5))(withDBNoImplicits.mysql)
      check(t.map(_ => Expr.StringLit("ałajć").charLength).toQueryIR.toSQLString(), checkValue(5))(withDBNoImplicits.mysql)
      check(t.map(_ => Expr.StringLit("ałajć").byteLength).toQueryIR.toSQLString(), checkValue(7))(withDBNoImplicits.mysql)
    }
    {
      import Dialect.mariadb.given
      check(t.map(_ => Expr.StringLit("ałajć").length).toQueryIR.toSQLString(), checkValue(5))(withDBNoImplicits.mariadb)
      check(t.map(_ => Expr.StringLit("ałajć").charLength).toQueryIR.toSQLString(), checkValue(5))(withDBNoImplicits.mariadb)
      check(t.map(_ => Expr.StringLit("ałajć").byteLength).toQueryIR.toSQLString(), checkValue(7))(withDBNoImplicits.mariadb)
    }
    {
      import Dialect.duckdb.given
      check(t.map(_ => Expr.StringLit("ałajć").length).toQueryIR.toSQLString(), checkValue(5))(withDBNoImplicits.duckdb)
      check(t.map(_ => Expr.StringLit("ałajć").charLength).toQueryIR.toSQLString(), checkValue(5))(withDBNoImplicits.duckdb)
      check(t.map(_ => Expr.StringLit("ałajć").byteLength).toQueryIR.toSQLString(), checkValue(7))(withDBNoImplicits.duckdb)
    }
    {
      import Dialect.h2.given
      check(t.map(_ => Expr.StringLit("ałajć").length).toQueryIR.toSQLString(), checkValue(5))(withDBNoImplicits.h2)
      check(t.map(_ => Expr.StringLit("ałajć").charLength).toQueryIR.toSQLString(), checkValue(5))(withDBNoImplicits.h2)
      check(t.map(_ => Expr.StringLit("ałajć").byteLength).toQueryIR.toSQLString(), checkValue(7))(withDBNoImplicits.h2)
    }
    {
      import Dialect.sqlite.given
      check(t.map(_ => Expr.StringLit("ałajć").length).toQueryIR.toSQLString(), checkValue(5))(withDBNoImplicits.sqlite)
      check(t.map(_ => Expr.StringLit("ałajć").charLength).toQueryIR.toSQLString(), checkValue(5))(withDBNoImplicits.sqlite)
      check(t.map(_ => Expr.StringLit("ałajć").byteLength).toQueryIR.toSQLString(), checkValue(7))(withDBNoImplicits.sqlite)
    }
  }
}
