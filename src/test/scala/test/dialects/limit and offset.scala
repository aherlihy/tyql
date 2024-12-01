package test.integration.limitandoffset

import munit.FunSuite
import test.TestDatabase
import tyql.Dialect
import test.SQLStringQueryTest
import test.query.{AllCommerceDBs, ShippingInfo, commerceDBs, Buyer}
import tyql.*
import language.experimental.namedTuples
import NamedTuple.*
import scala.language.implicitConversions

private val mysqlDialects = Set(
  Dialect.mysql.given_Dialect,
  Dialect.mariadb.given_Dialect,
)
private val otherDialects = Set(
  Dialect.ansi.given_Dialect,
  Dialect.postgresql.given_Dialect,
  Dialect.h2.given_Dialect,
  Dialect.duckdb.given_Dialect,
  Dialect.sqlite.given_Dialect,
)
private val dialects = mysqlDialects ++ otherDialects

class JustLimitUnaffected extends SQLStringQueryTest[AllCommerceDBs, String] {
  override def testDescription: String = "Just LIMIT remains uniform across all dialects"
  def query() = testDB.tables.products.map { c => c.name }.limit(10)
  def expectedQueryPattern = """SELECT product$A.name FROM product as product$A LIMIT 10"""
}

class JustOffsetUnaffected extends SQLStringQueryTest[AllCommerceDBs, String] {
  override def testDescription: String = "Just OFFSET remains uniform across all dialects"
  def query() = testDB.tables.products.map { c => c.name }.offset(30)
  def expectedQueryPattern = """SELECT product$A.name FROM product as product$A OFFSET 30"""
}

class BothTogetherOnMySQL extends SQLStringQueryTest[AllCommerceDBs, String] {
  override def munitIgnore: Boolean = true
  override def testDescription: String = "LIMIT+OFFSET is special on MySQL"
  def query() =
    import Dialect.mysql.given
    testDB.tables.products.map { c => c.name }.limit(10).offset(30)
  def expectedQueryPattern = """SELECT product$A.name FROM product as product$A LIMIT 30,10"""
}

class BothTogetherOnMariaDB extends SQLStringQueryTest[AllCommerceDBs, String] {
  override def munitIgnore: Boolean = true
  override def testDescription: String = "LIMIT+OFFSET is special on MySQL"
  def query() =
    import Dialect.mysql.given
    testDB.tables.products.map { c => c.name }.limit(10).offset(30)
  def expectedQueryPattern = """SELECT product$A.name FROM product as product$A LIMIT 30,10"""
}

class SeparateOnPostgresql extends SQLStringQueryTest[AllCommerceDBs, String] {
  override def testDescription: String = "LIMIT+OFFSET is special on MySQL"
  def query() =
    import Dialect.postgresql.given
    testDB.tables.products.map { c => c.name }.limit(10).offset(30)
  def expectedQueryPattern = """SELECT product$A.name FROM product as product$A  LIMIT 10 OFFSET 30"""
}

class SeparateOnSqlite extends SQLStringQueryTest[AllCommerceDBs, String] {
  override def testDescription: String = "LIMIT+OFFSET is special on MySQL"
  def query() =
    import Dialect.sqlite.given
    testDB.tables.products.map { c => c.name }.limit(10).offset(30)
  def expectedQueryPattern = """SELECT product$A.name FROM product as product$A LIMIT 10 OFFSET 30"""
}

// TODO parametrize these tests better later
