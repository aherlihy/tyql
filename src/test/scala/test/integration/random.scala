package test.integration.random

import munit.FunSuite
import test.{withDB, checkExprDialect}
import java.sql.{Connection, Statement, ResultSet}
import tyql.{Dialect, Table, Expr, lit}

class RandomTests extends FunSuite {
  test("randomFloat test") {
    val checkValue = { (rs: ResultSet) =>
      val r = rs.getDouble(1)
      assert(0 <= r && r <= 1)
    }

    {
      import Dialect.postgresql.given
      checkExprDialect[Double](Expr.randomFloat(), checkValue)(withDB.postgres)
    }
    {
      import Dialect.mysql.given
      checkExprDialect[Double](Expr.randomFloat(), checkValue)(withDB.mysql)
    }
    {
      import Dialect.mariadb.given
      checkExprDialect[Double](Expr.randomFloat(), checkValue)(withDB.mariadb)
    }
    {
      import Dialect.duckdb.given
      checkExprDialect[Double](Expr.randomFloat(), checkValue)(withDB.duckdb)
    }
    {
      import Dialect.h2.given
      checkExprDialect[Double](Expr.randomFloat(), checkValue)(withDB.h2)
    }
    {
      import Dialect.sqlite.given
      checkExprDialect[Double](Expr.randomFloat(), checkValue)(withDB.sqlite)
    }
  }

  test("randomFloat late binding") {
    val checkValue = { (rs: ResultSet) =>
      val r = rs.getDouble(1)
      assert(0 <= r && r <= 1)
    }

    var q: tyql.Expr[Double, tyql.NonScalarExpr] = null
    {
      // XXX you can program against a feature set, not any specific dialect!
      // TODO but the syntax for now is ugly...
      import tyql.DialectFeature.RandomFloat
      given RandomFloat = new RandomFloat {}
      q = Expr.randomFloat()
    }

    {
      import Dialect.postgresql.given
      checkExprDialect[Double](q, checkValue, s => assert(s.toLowerCase().contains("random(")))(withDB.postgres)
    }
    {
      import Dialect.mysql.given
      checkExprDialect[Double](q, checkValue, s => assert(s.toLowerCase().contains("rand(")))(withDB.mysql)
    }
  }

  test("randomUUID test") {
    val checkValue = { (rs: ResultSet) =>
      val r = rs.getString(1)
      assert(r.matches("[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"))
    }

    {
      import Dialect.postgresql.given
      checkExprDialect[String](Expr.randomUUID(), checkValue)(withDB.postgres)
    }
    {
      import Dialect.mysql.given
      checkExprDialect[String](Expr.randomUUID(), checkValue)(withDB.mysql)
    }
    {
      import Dialect.mariadb.given
      checkExprDialect[String](Expr.randomUUID(), checkValue)(withDB.mariadb)
    }
    {
      import Dialect.duckdb.given
      checkExprDialect[String](Expr.randomUUID(), checkValue)(withDB.duckdb)
    }
    {
      import Dialect.h2.given
      checkExprDialect[String](Expr.randomUUID(), checkValue)(withDB.h2)
    }
  }

  test("randomUUID late binding") {
    val checkValue = { (rs: ResultSet) =>
      val r = rs.getString(1)
      assert(r.matches("[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"))
    }

    var q: tyql.Expr[String, tyql.NonScalarExpr] = null
    {
      import tyql.DialectFeature.RandomUUID
      given RandomUUID = new RandomUUID {}
      q = Expr.randomUUID()
    }

    {
      import Dialect.postgresql.given
      checkExprDialect[String](q, checkValue, s => assert(s.toLowerCase().contains("gen_random_uuid(")))(withDB.postgres)
    }
    {
      import Dialect.mysql.given
      checkExprDialect[String](q, checkValue, s => assert(s.toLowerCase().contains("uuid(")))(withDB.mysql)
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
      assertEquals(r, 44)
    }

    {
      import Dialect.postgresql.given
      checkExprDialect[Int](Expr.randomInt(0, lit(1) + lit(1)), checkValue)(withDB.postgres)
      checkExprDialect[Int](Expr.randomInt(lit(20) + lit(24), 44), checkInclusion)(withDB.postgres)
    }
    {
      import Dialect.mysql.given
      checkExprDialect[Int](Expr.randomInt(tyql.lit(0), lit(1) + lit(1)), checkValue)(withDB.mysql)
      checkExprDialect[Int](Expr.randomInt(lit(22) + lit(22), 44), checkInclusion)(withDB.mysql)
    }
    {
      import Dialect.mariadb.given
      checkExprDialect[Int](Expr.randomInt(0, lit(1) + lit(1)), checkValue)(withDB.mariadb)
      checkExprDialect[Int](Expr.randomInt(lit(20) + lit(24), 44), checkInclusion)(withDB.mariadb)
    }
    {
      import Dialect.duckdb.given
      checkExprDialect[Int](Expr.randomInt(0, lit(1) + lit(1)), checkValue)(withDB.duckdb)
      checkExprDialect[Int](Expr.randomInt(lit(20) + lit(24), tyql.lit(44)), checkInclusion)(withDB.duckdb)
    }
    {
      import Dialect.h2.given
      checkExprDialect[Int](Expr.randomInt(0, lit(1) + lit(1)), checkValue)(withDB.h2)
      checkExprDialect[Int](Expr.randomInt(lit(22) + lit(22), 44), checkInclusion)(withDB.h2)
    }
    {
      import Dialect.sqlite.given
      checkExprDialect[Int](Expr.randomInt(0, lit(1) + lit(1)), checkValue)(withDB.sqlite)
      checkExprDialect[Int](Expr.randomInt(lit(20) + lit(24), 44), checkInclusion)(withDB.sqlite)
    }
  }

  test("randomInt late binding") {
    val checkInclusion = { (rs: ResultSet) =>
      val r = rs.getInt(1)
      assertEquals(r, 101)
    }

    var q: tyql.Expr[Int, tyql.NonScalarExpr] = null
    {
      import tyql.DialectFeature.RandomIntegerInInclusiveRange
      given RandomIntegerInInclusiveRange = new RandomIntegerInInclusiveRange {}
      q = Expr.randomInt(tyql.lit(101), tyql.lit(101))
    }

    {
      import Dialect.h2.given
      checkExprDialect[Int](q, checkInclusion, s => assert(s.toLowerCase().contains("floor(")))(withDB.h2)
    }
    {
      import Dialect.sqlite.given
      checkExprDialect[Int](q, checkInclusion, s => assert(s.toLowerCase().contains("cast(")))(withDB.sqlite)
    }
  }
}
