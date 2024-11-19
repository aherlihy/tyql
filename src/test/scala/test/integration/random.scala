package test.integration.random

import munit.FunSuite
import test.withDBNoImplicits
import java.sql.{Connection, Statement}
import tyql.{Dialect, Table, Expr}

class RandomTests extends FunSuite {
  test("Random test") {

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

    def check(sqlQuery: String)(runner: (f: Connection => Unit) => Unit): Unit = {
      for (_ <- 1 to 3) {
        runner { conn =>
          val stmt = conn.createStatement()
          init(stmt)
          val rs = stmt.executeQuery(sqlQuery)
          assert(rs.next())
          val r = rs.getDouble(1)
          assert(0 <= r && r <= 1)
          clean(stmt)
        }
      }
    }
    
    {
      import Dialect.postgresql.given
      check(t.map(_ => Expr.randomFloat()).toQueryIR.toSQLString())(withDBNoImplicits.postgres)
    }
    {
      import Dialect.mysql.given
      check(t.map(_ => Expr.randomFloat()).toQueryIR.toSQLString())(withDBNoImplicits.mysql)
    }
    {
      import Dialect.mariadb.given
      check(t.map(_ => Expr.randomFloat()).toQueryIR.toSQLString())(withDBNoImplicits.mariadb)
    }
    {
      import Dialect.duckdb.given
      check(t.map(_ => Expr.randomFloat()).toQueryIR.toSQLString())(withDBNoImplicits.duckdb)
    }
    {
      import Dialect.h2.given
      check(t.map(_ => Expr.randomFloat()).toQueryIR.toSQLString())(withDBNoImplicits.h2)
    }
    {
      import Dialect.sqlite.given
      check(t.map(_ => Expr.randomFloat()).toQueryIR.toSQLString())(withDBNoImplicits.sqlite)
    }
  }
}
