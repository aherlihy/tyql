package test

import munit.FunSuite
import tyql.Dialect
import java.sql.{Connection, DriverManager}
import test.needsDBs

private def withConnection[A]
  (url: String, user: String = "", password: String = "")
  (d: Dialect)
  (f: Connection => Dialect ?=> A)
  : A = {
  var conn: Connection = null
  try {
    conn = DriverManager.getConnection(url, user, password)
    f(conn)(using d)
  } finally {
    if (conn != null) conn.close()
  }
}

private def withConnectionNoImplicits[A]
  (url: String, user: String = "", password: String = "")
  (f: Connection => A)
  : A = {
  var conn: Connection = null
  try {
    conn = DriverManager.getConnection(url, user, password)
    f(conn)
  } finally {
    if (conn != null) conn.close()
  }
}

object withDB:
  def postgres[A](f: Connection => Dialect ?=> A): A = {
    // XXX postgres driver needs to be poked first
    // https://github.com/pgjdbc/pgjdbc/pull/772
    org.postgresql.Driver.isRegistered()
    withConnection(
      "jdbc:postgresql://localhost:5433/testdb",
      "testuser",
      "testpass"
    )(tyql.Dialect.postgresql.given_Dialect)(f)
  }

  def mysql[A](f: Connection => Dialect ?=> A): A = {
    given Dialect = tyql.Dialect.mysql.given_Dialect
    withConnection(
      "jdbc:mysql://localhost:3307/testdb",
      "testuser",
      "testpass"
    )(tyql.Dialect.mysql.given_Dialect)(f)
  }

  def mariadb[A](f: Connection => Dialect ?=> A)(using Dialect): A = {
    given Dialect = tyql.Dialect.mariadb.given_Dialect
    withConnection(
      "jdbc:mariadb://localhost:3308/testdb",
      "testuser",
      "testpass"
    )(tyql.Dialect.mariadb.given_Dialect)(f)
  }

  def sqlite[A](f: Connection => Dialect ?=> A)(using Dialect): A = {
    given Dialect = tyql.Dialect.sqlite.given_Dialect
    withConnection(
      "jdbc:sqlite::memory:"
    )(tyql.Dialect.sqlite.given_Dialect)(f)
  }

  def duckdb[A](f: Connection => Dialect ?=> A): A = {
    given Dialect = tyql.Dialect.duckdb.given_Dialect
    withConnection(
      "jdbc:duckdb:"
    )(tyql.Dialect.duckdb.given_Dialect)(f)
  }

  def h2[A](f: Connection => Dialect ?=> A): A = {
    given Dialect = tyql.Dialect.h2.given_Dialect
    withConnection(
      "jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1"
    )(tyql.Dialect.h2.given_Dialect)(f)
  }

  def all[A](f: Connection => Dialect ?=> A): Unit = {
    postgres(f)
    mysql(f)
    mariadb(f)
    sqlite(f)
    duckdb(f)
    h2(f)
  }

  def allmysql[A](f: Connection => Dialect ?=> A): Unit = {
    mysql(f)
    mariadb(f)
  }

object withDBNoImplicits:
  def postgres[A](f: Connection => A): A = {
    // XXX postgres driver needs to be poked first
    // https://github.com/pgjdbc/pgjdbc/pull/772
    org.postgresql.Driver.isRegistered()
    withConnectionNoImplicits(
      "jdbc:postgresql://localhost:5433/testdb",
      "testuser",
      "testpass"
    )(f)
  }

  def mysql[A](f: Connection => A): A = {
    given Dialect = tyql.Dialect.mysql.given_Dialect
    withConnectionNoImplicits(
      "jdbc:mysql://localhost:3307/testdb",
      "testuser",
      "testpass"
    )(f)
  }

  def mariadb[A](f: Connection => A)(using Dialect): A = {
    given Dialect = tyql.Dialect.mariadb.given_Dialect
    withConnectionNoImplicits(
      "jdbc:mariadb://localhost:3308/testdb",
      "testuser",
      "testpass"
    )(f)
  }

  def sqlite[A](f: Connection => A)(using Dialect): A = {
    given Dialect = tyql.Dialect.sqlite.given_Dialect
    withConnectionNoImplicits(
      "jdbc:sqlite::memory:"
    )(f)
  }

  def duckdb[A](f: Connection => A): A = {
    given Dialect = tyql.Dialect.duckdb.given_Dialect
    withConnectionNoImplicits(
      "jdbc:duckdb:"
    )(f)
  }

  def h2[A](f: Connection => A): A = {
    given Dialect = tyql.Dialect.h2.given_Dialect
    withConnectionNoImplicits(
      "jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1"
    )(f)
  }

  def all[A](f: Connection => A): Unit = {
    postgres(f)
    mysql(f)
    mariadb(f)
    sqlite(f)
    duckdb(f)
    h2(f)
  }

  def allmysql[A](f: Connection => A): Unit = {
    mysql(f)
    mariadb(f)
  }

class WithDBHelpersTest extends FunSuite {
  private def checkSelect10(conn: Connection) = {
    val stmt = conn.createStatement()
    val rs = stmt.executeQuery("SELECT 10 as x")
    assert(rs.next())
    val x = rs.getInt("x")
    assertEquals(x, 10)
  }

  test("dialect selection works with withDB helper methods".tag(needsDBs)) {
    withDB.postgres { conn =>
      checkSelect10(conn)
      assertEquals(summon[Dialect].name(), "PostgreSQL Dialect")
    }
    withDB.sqlite { conn =>
      checkSelect10(conn)
      assertEquals(summon[Dialect].name(), "SQLite Dialect")
    }
    withDB.mysql { conn =>
      checkSelect10(conn)
      assertEquals(summon[Dialect].name(), "MySQL Dialect")
    }
    withDB.mariadb { conn =>
      checkSelect10(conn)
      assertEquals(summon[Dialect].name(), "MariaDB Dialect")
    }
    withDB.h2 { conn =>
      checkSelect10(conn)
      assertEquals(summon[Dialect].name(), "H2 Dialect")
    }
    withDB.duckdb { conn =>
      checkSelect10(conn)
      assertEquals(summon[Dialect].name(), "DuckDB Dialect")
    }
  }

  test("withDBNoImplicits helper methods do not inject implicits".tag(needsDBs)) {
    withDBNoImplicits.all { conn =>
      checkSelect10(conn)
      assertEquals(summon[Dialect].name(), "ANSI SQL Dialect")
    }
  }
}
