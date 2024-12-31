package test

import munit.FunSuite
import test.withDBNoImplicits
import java.sql.{Connection, Statement, ResultSet}
import tyql.{Dialect, Table, Expr}
import tyql.{NonScalarExpr, ResultTag}

def checkExpr[A]
  (using ResultTag[A])
  (expr: tyql.Expr[A, NonScalarExpr], checkValue: ResultSet => Unit, sqlCallback: String => Unit = _ => ())
  (runner: (f: Connection => Unit) => Unit)
  : Unit = {
  case class Row(i: Int)
  val t = Table[Row]("table59175810544")
  runner { conn =>
    val stmt = conn.createStatement()
    stmt.executeUpdate(s"DROP TABLE IF EXISTS table59175810544;")
    stmt.executeUpdate(s"CREATE TABLE table59175810544 (i INTEGER);")
    stmt.executeUpdate(s"INSERT INTO table59175810544 (i) VALUES (1);")
    val sqlQueryString = t.map(_ => expr).toQueryIR.toSQLString()
    sqlCallback(sqlQueryString)
    val rs = stmt.executeQuery(sqlQueryString)
    assert(rs.next())
    checkValue(rs)
    stmt.executeUpdate(s"DROP TABLE IF EXISTS table59175810544;")
  }
}

def checkExprDialect[A]
  (using ResultTag[A])
  (expr: tyql.Expr[A, NonScalarExpr], checkValue: ResultSet => Unit, sqlCallback: String => Unit = _ => ())
  (runner: (f: Connection => Dialect ?=> Unit) => Unit)
  : Unit = {
  case class Row(i: Int)
  val t = Table[Row]("table59175810544")
  runner { conn =>
    val stmt = conn.createStatement()
    stmt.executeUpdate(s"DROP TABLE IF EXISTS table59175810544;")
    stmt.executeUpdate(s"CREATE TABLE table59175810544 (i INTEGER);")
    stmt.executeUpdate(s"INSERT INTO table59175810544 (i) VALUES (1);")
    val sqlQueryString = t.map(_ => expr).toQueryIR.toSQLString()
    sqlCallback(sqlQueryString)
    val rs = stmt.executeQuery(sqlQueryString)
    assert(rs.next())
    checkValue(rs)
    stmt.executeUpdate(s"DROP TABLE IF EXISTS table59175810544;")
  }
}
