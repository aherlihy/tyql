package test

import munit.FunSuite
import test.withDBNoImplicits
import java.sql.{Connection, Statement, ResultSet}
import tyql.{Dialect, Table, Expr}
import tyql.Subset.a
import tyql.{NonScalarExpr, ResultTag}


def checkExpr[A](using ResultTag[A])
    (expr: tyql.Expr[A, NonScalarExpr], checkValue: ResultSet => Unit)
    (runner: (f: Connection => Unit) => Unit): Unit = {
  case class Row(i: Int)
  val t = Table[Row]("table59175810544")
  runner { conn =>
    val stmt = conn.createStatement()
    stmt.executeUpdate(s"DROP TABLE IF EXISTS table59175810544;")
    stmt.executeUpdate(s"CREATE TABLE table59175810544 (i INTEGER);")
    stmt.executeUpdate(s"INSERT INTO table59175810544 (i) VALUES (1);")
    val rs = stmt.executeQuery(t.map(_ => expr).toQueryIR.toSQLString())
    assert(rs.next())
    checkValue(rs)
    stmt.executeUpdate(s"DROP TABLE IF EXISTS table59175810544;")
  }
}
