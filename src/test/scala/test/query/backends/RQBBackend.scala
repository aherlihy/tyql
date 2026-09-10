package test.query.backends

import test.TestComparitor
import tyql.DatabaseAST
import tyql.dialects.Dialect

/**
 * The four RDBMS the Recursive Query Benchmark is reported against.
 *
 * A backend is selected exactly the way a user would select one: by putting its
 * `Dialect` given in scope. In application code that is an import
 * (`import tyql.dialects.postgresql.given`); here the dialect is passed
 * explicitly so one test class can cover all four.
 */
enum Backend(val label: String, val dialect: Dialect):
  case DuckDB extends Backend("DuckDB", tyql.dialects.duckdb.duck)
  case Postgres extends Backend("Postgres", tyql.dialects.postgresql.postgres)
  case SQLite extends Backend("SQLite", tyql.dialects.sqlite.sqlite)
  case MariaDB extends Backend("MariaDB", tyql.dialects.mariadb.maria)

/**
 * One RQB query, checked against the SQL it must generate on each backend.
 *
 * Expected strings are compared exactly (modulo whitespace and TyQL's generated
 * alias numbers, which `$`-placeholders stand in for) in the same way as
 * `test.query.recursivebenchmarks`. A backend that needs no engine-specific SQL
 * inherits the DuckDB string, so an override is a positive statement that this
 * query is rendered differently there.
 *
 * The reference these are checked against is `rqb_benchmark/queries/<q>/query.sql`,
 * or `query-<db>.sql` where the benchmark ships an engine-specific variant.
 */
abstract class RQBBackendTest extends munit.FunSuite:
  def testDescription: String

  /** Rebuilt for each backend: alias numbers are assigned during IR construction. */
  def query(): DatabaseAST[?]

  def duckdb: String
  def postgres: String = duckdb
  def sqlite: String = duckdb
  def mariadb: String = duckdb

  private def expectedFor(b: Backend): String = b match
    case Backend.DuckDB => duckdb
    case Backend.Postgres => postgres
    case Backend.SQLite => sqlite
    case Backend.MariaDB => mariadb

  private def normalize(s: String): String =
    s.trim().replace("\n", " ").replaceAll("\\s+", " ")

  Backend.values.foreach: backend =>
    test(s"$testDescription [${backend.label}]"):
      val actual = normalize(query().toSQLString(using backend.dialect))
      val (success, debug) = TestComparitor.matchStrings(normalize(expectedFor(backend)), actual)
      if !success then
        println(s"=== ${backend.label}: $testDescription")
        println(actual)
      assert(success, debug)
