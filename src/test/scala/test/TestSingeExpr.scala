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
  runner { conn =>
    val stmt = conn.createStatement()
    val sqlQueryString = tyql.Exprs[(a: A)](Tuple1(expr)).toQueryIR.toSQLString()
    sqlCallback(sqlQueryString)
    val rs = stmt.executeQuery(sqlQueryString)
    assert(rs.next())
    checkValue(rs)
  }
}

def checkExprDialect[A]
  (using ResultTag[A])
  (expr: tyql.Expr[A, NonScalarExpr], checkValue: ResultSet => Unit, sqlCallback: String => Unit = _ => ())
  (runner: (f: Connection => Dialect ?=> Unit) => Unit)
  : Unit = {
  runner { conn =>
    val stmt = conn.createStatement()
    val sqlQueryString = tyql.Exprs[(a: A)](Tuple1(expr)).toQueryIR.toSQLString()
    sqlCallback(sqlQueryString)
    val rs = stmt.executeQuery(sqlQueryString)
    assert(rs.next())
    checkValue(rs)
  }
}
