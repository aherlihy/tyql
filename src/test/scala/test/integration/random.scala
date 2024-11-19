package test.integration.random

import munit.FunSuite
import test.withDBNoImplicits
import java.sql.{Connection, Statement, ResultSet}
import tyql.{Dialect, Table, Expr}
import tyql.Subset.a

class RandomTests extends FunSuite {
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

  test("randomFloat test") {
    val checkValue = { (rs: ResultSet) =>
      assert(rs.next())
      val r = rs.getDouble(1)
      assert(0 <= r && r <= 1)
    }

    {
      import Dialect.postgresql.given
      check(t.map(_ => Expr.randomFloat()).toQueryIR.toSQLString(), checkValue)(withDBNoImplicits.postgres)
    }
    {
      import Dialect.mysql.given
      check(t.map(_ => Expr.randomFloat()).toQueryIR.toSQLString(), checkValue)(withDBNoImplicits.mysql)
    }
    {
      import Dialect.mariadb.given
      check(t.map(_ => Expr.randomFloat()).toQueryIR.toSQLString(), checkValue)(withDBNoImplicits.mariadb)
    }
    {
      import Dialect.duckdb.given
      check(t.map(_ => Expr.randomFloat()).toQueryIR.toSQLString(), checkValue)(withDBNoImplicits.duckdb)
    }
    {
      import Dialect.h2.given
      check(t.map(_ => Expr.randomFloat()).toQueryIR.toSQLString(), checkValue)(withDBNoImplicits.h2)
    }
    {
      import Dialect.sqlite.given
      check(t.map(_ => Expr.randomFloat()).toQueryIR.toSQLString(), checkValue)(withDBNoImplicits.sqlite)
    }
  }

  test("randomUUID test") {
    val checkValue = { (rs: ResultSet) =>
      assert(rs.next())
      val r = rs.getString(1)
      assert(r.matches("[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"))
    }

    {
      import Dialect.postgresql.given
      check(t.map(_ => Expr.randomUUID()).toQueryIR.toSQLString(), checkValue)(withDBNoImplicits.postgres)
    }
    {
      import Dialect.mysql.given
      check(t.map(_ => Expr.randomUUID()).toQueryIR.toSQLString(), checkValue)(withDBNoImplicits.mysql)
    }
    {
      import Dialect.mariadb.given
      check(t.map(_ => Expr.randomUUID()).toQueryIR.toSQLString(), checkValue)(withDBNoImplicits.mariadb)
    }
    {
      import Dialect.duckdb.given
      check(t.map(_ => Expr.randomUUID()).toQueryIR.toSQLString(), checkValue)(withDBNoImplicits.duckdb)
    }
    {
      import Dialect.h2.given
      check(t.map(_ => Expr.randomUUID()).toQueryIR.toSQLString(), checkValue)(withDBNoImplicits.h2)
    }
  }

  test("randomInt test") {
    import scala.language.implicitConversions

    val checkValue = { (rs: ResultSet) =>
      assert(rs.next())
      val r = rs.getInt(1)
      assert(0 <= r && r <= 2)
    }

    val checkInclusion = { (rs: ResultSet) =>
      assert(rs.next())
      val r = rs.getInt(1)
      assert(r == 44)
    }

    {
      import Dialect.postgresql.given
      check(t.map(_ => Expr.randomInt(0, 2)).toQueryIR.toSQLString(), checkValue)(withDBNoImplicits.postgres)
      check(t.map(_ => Expr.randomInt(44, 44)).toQueryIR.toSQLString(), checkInclusion)(withDBNoImplicits.postgres)
    }
    {
      import Dialect.mysql.given
      check(t.map(_ => Expr.randomInt(0, 2)).toQueryIR.toSQLString(), checkValue)(withDBNoImplicits.mysql)
      check(t.map(_ => Expr.randomInt(44, 44)).toQueryIR.toSQLString(), checkInclusion)(withDBNoImplicits.mysql)
    }
    {
      import Dialect.mariadb.given
      check(t.map(_ => Expr.randomInt(0, 2)).toQueryIR.toSQLString(), checkValue)(withDBNoImplicits.mariadb)
      check(t.map(_ => Expr.randomInt(44, 44)).toQueryIR.toSQLString(), checkInclusion)(withDBNoImplicits.mariadb)
    }
    {
      import Dialect.duckdb.given
      check(t.map(_ => Expr.randomInt(0, 2)).toQueryIR.toSQLString(), checkValue)(withDBNoImplicits.duckdb)
      check(t.map(_ => Expr.randomInt(44, 44)).toQueryIR.toSQLString(), checkInclusion)(withDBNoImplicits.duckdb)
    }
    {
      import Dialect.h2.given
      check(t.map(_ => Expr.randomInt(0, 2)).toQueryIR.toSQLString(), checkValue)(withDBNoImplicits.h2)
      check(t.map(_ => Expr.randomInt(44, 44)).toQueryIR.toSQLString(), checkInclusion)(withDBNoImplicits.h2)
    }
    {
      import Dialect.sqlite.given
      check(t.map(_ => Expr.randomInt(0, 2)).toQueryIR.toSQLString(), checkValue)(withDBNoImplicits.sqlite)
      check(t.map(_ => Expr.randomInt(44, 44)).toQueryIR.toSQLString(), checkInclusion)(withDBNoImplicits.sqlite)
    }
  }
}
