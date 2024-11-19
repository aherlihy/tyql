package test.integration.random

import munit.FunSuite
import test.{withDBNoImplicits, checkExpr}
import java.sql.{Connection, Statement, ResultSet}
import tyql.{Dialect, Table, Expr}
import tyql.Subset.a

class RandomTests extends FunSuite {
  test("randomFloat test") {
    val checkValue = { (rs: ResultSet) =>
      val r = rs.getDouble(1)
      assert(0 <= r && r <= 1)
    }

    {
      import Dialect.postgresql.given
      checkExpr[Double](Expr.randomFloat(), checkValue)(withDBNoImplicits.postgres)
    }
    {
      import Dialect.mysql.given
      checkExpr[Double](Expr.randomFloat(), checkValue)(withDBNoImplicits.mysql)
    }
    {
      import Dialect.mariadb.given
      checkExpr[Double](Expr.randomFloat(), checkValue)(withDBNoImplicits.mariadb)
    }
    {
      import Dialect.duckdb.given
      checkExpr[Double](Expr.randomFloat(), checkValue)(withDBNoImplicits.duckdb)
    }
    {
      import Dialect.h2.given
      checkExpr[Double](Expr.randomFloat(), checkValue)(withDBNoImplicits.h2)
    }
    {
      import Dialect.sqlite.given
      checkExpr[Double](Expr.randomFloat(), checkValue)(withDBNoImplicits.sqlite)
    }
  }

  test("randomUUID test") {
    val checkValue = { (rs: ResultSet) =>
      val r = rs.getString(1)
      assert(r.matches("[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"))
    }

    {
      import Dialect.postgresql.given
      checkExpr[String](Expr.randomUUID(), checkValue)(withDBNoImplicits.postgres)
    }
    {
      import Dialect.mysql.given
      checkExpr[String](Expr.randomUUID(), checkValue)(withDBNoImplicits.mysql)
    }
    {
      import Dialect.mariadb.given
      checkExpr[String](Expr.randomUUID(), checkValue)(withDBNoImplicits.mariadb)
    }
    {
      import Dialect.duckdb.given
      checkExpr[String](Expr.randomUUID(), checkValue)(withDBNoImplicits.duckdb)
    }
    {
      import Dialect.h2.given
      checkExpr[String](Expr.randomUUID(), checkValue)(withDBNoImplicits.h2)
    }
  }

  test("randomInt test") {
    import scala.language.implicitConversions

    val checkValue = { (rs: ResultSet) =>
      val r = rs.getInt(1)
      assert(0 <= r && r <= 2)
    }

    val checkInclusion = { (rs: ResultSet) =>
      val r = rs.getInt(1)
      assert(r == 44)
    }

    {
      import Dialect.postgresql.given
      checkExpr[Int](Expr.randomInt(0, 2), checkValue)(withDBNoImplicits.postgres)
      checkExpr[Int](Expr.randomInt(44, 44), checkInclusion)(withDBNoImplicits.postgres)
    }
    {
      import Dialect.mysql.given
      checkExpr[Int](Expr.randomInt(0, 2), checkValue)(withDBNoImplicits.mysql)
      checkExpr[Int](Expr.randomInt(44, 44), checkInclusion)(withDBNoImplicits.mysql)
    }
    {
      import Dialect.mariadb.given
      checkExpr[Int](Expr.randomInt(0, 2), checkValue)(withDBNoImplicits.mariadb)
      checkExpr[Int](Expr.randomInt(44, 44), checkInclusion)(withDBNoImplicits.mariadb)
    }
    {
      import Dialect.duckdb.given
      checkExpr[Int](Expr.randomInt(0, 2), checkValue)(withDBNoImplicits.duckdb)
      checkExpr[Int](Expr.randomInt(44, 44), checkInclusion)(withDBNoImplicits.duckdb)
    }
    {
      import Dialect.h2.given
      checkExpr[Int](Expr.randomInt(0, 2), checkValue)(withDBNoImplicits.h2)
      checkExpr[Int](Expr.randomInt(44, 44), checkInclusion)(withDBNoImplicits.h2)
    }
    {
      import Dialect.sqlite.given
      checkExpr[Int](Expr.randomInt(0, 2), checkValue)(withDBNoImplicits.sqlite)
      checkExpr[Int](Expr.randomInt(44, 44), checkInclusion)(withDBNoImplicits.sqlite)
    }
  }
}
