package test.query.backends

import test.query.recursivebenchmarks.{given, *}
import tyql.Query.unrestrictedBagFix

import language.experimental.namedTuples
import NamedTuple.*

/**
 * The user-facing way to pick a backend: put its `Dialect` given in scope with
 * an import. The same TyQL source then renders as different SQL.
 */
class DialectSelectionTest extends munit.FunSuite:

  /** A query with a list-valued column and a non-linear-free recursive term,
   *  so every dialect knob this project adds is exercised. */
  private def tcQuery() =
    GraphDBs.tables.edge
      .filter(p => p.x == 1)
      .map(e => (startNode = e.x, endNode = e.y, path = List(e.x, e.y).toExpr).toRow)
      .unrestrictedBagFix(path =>
        path.flatMap(p =>
          GraphDBs.tables.edge
            .filter(e => e.x == p.endNode && !p.path.contains(e.y))
            .map(e => (startNode = p.startNode, endNode = e.y, path = p.path.append(e.y)).toRow)))

  test("no import: the default given renders DuckDB"):
    val sql = tcQuery().toSQLString
    assert(sql.contains("list_append("), sql)

  test("import duckdb.given: native list values"):
    import tyql.dialects.duckdb.given
    val sql = tcQuery().toSQLString
    assert(sql.contains("list_append(") && sql.contains("list_contains("), sql)

  test("import postgresql.given: list encoded as a delimited string with ||"):
    import tyql.dialects.postgresql.given
    val sql = tcQuery().toSQLString
    assert(!sql.contains("list_append("), sql)
    assert(sql.contains("|| ',' as path"), sql)
    assert(sql.contains("LIKE '%,'"), sql)

  test("import sqlite.given: no parenthesized compound branch"):
    import tyql.dialects.sqlite.given
    val sql = tcQuery().toSQLString
    assert(sql.contains("UNION ALL SELECT"), sql)
    assert(!sql.contains("UNION ALL ((SELECT"), sql)

  test("import mariadb.given: CONCAT rather than ||"):
    import tyql.dialects.mariadb.given
    val sql = tcQuery().toSQLString
    assert(!sql.contains("||"), sql)
    assert(sql.contains("CONCAT("), sql)
    assert(sql.contains("AS CHAR(4000))"), sql)

  test("the four backends generate four distinct strings"):
    val sqls =
      List(
        tyql.dialects.duckdb.duck,
        tyql.dialects.postgresql.postgres,
        tyql.dialects.sqlite.sqlite,
        tyql.dialects.mariadb.maria
      ).map(d => tcQuery().toSQLString(using d))
    assertEquals(sqls.distinct.size, 4, sqls.mkString("\n"))
