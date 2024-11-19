package test.dialects.cachingworksbetweendialectsandconfigs

import munit.FunSuite

class QueryCachingWorksBetweenDialectsAndConfigs extends FunSuite {

  case class Row(i: Int)
  val t = tyql.Table[Row]("t")

  // TODO this is currently broken
  test("query caching works between dialects and configs".ignore) {
    var q: tyql.QueryIRNode = null
    {
      q = t.map(_ => tyql.Expr.StringLit("abc").charLength).toQueryIR
    }
    {
      import tyql.Dialect.postgresql.given
      println(expr.toSQLString())
    }
    {
      import tyql.Dialect.mysql.given
      println(expr.toSQLString())
    }
  }
}
