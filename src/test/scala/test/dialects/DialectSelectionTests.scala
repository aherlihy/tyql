package test.integration.dialectselection

import munit.FunSuite
import tyql.Dialect

class DialectSelectionTests extends FunSuite {
  private def behavior()(using d: Dialect): String = d.name()

  test("ANSI selected by default") {
      assertEquals(behavior(), "ANSI SQL Dialect")
  }

  test("ANSI selected also by import") {
    import Dialect.ansi.given
    assertEquals(behavior(), "ANSI SQL Dialect")
  }

  test("All the other dialects can be selected") {
    {
      import Dialect.postgresql.given
      assertEquals(behavior(), "PostgreSQL Dialect")
    }
    {
      import Dialect.mariadb.given
      assertEquals(behavior(), "MariaDB Dialect")
    }
    {
      import Dialect.mysql.given
      assertEquals(behavior(), "MySQL Dialect")
    }
    {
      import Dialect.sqlite.given
      assertEquals(behavior(), "SQLite Dialect")
    }
    {
      import Dialect.h2.given
      assertEquals(behavior(), "H2 Dialect")
    }
    {
      import Dialect.duckdb.given
      assertEquals(behavior(), "DuckDB Dialect")
    }
  }
}
