package test.dialects.cachingworksbetweendialectsandconfigs

import munit.FunSuite
import tyql.Dialect

class QueryCachingWorksBetweenDialectsAndConfigsTests extends FunSuite {

  case class Row(i: Int)
  val t = tyql.Table[Row]("t")

  test("query caching between dialects") {
    // TODO XXX toQueryIR is dialect-dependent!!! Is this OK or not?
    // XXX by changing the number of calls and looking at printouts in the logs you can see that caching works
    var q: tyql.Query[?, ?] = null
    {
      import tyql.Dialect.postgresql.given
      q = t.map(_ => tyql.Expr.StringLit("abc").charLength)
      val ir = q.toQueryIR
      val ir2 = q.toQueryIR
      val ir3 = q.toQueryIR
      val s1 = ir.toSQLString()
      val s2 = ir.toSQLString()
      val s3 = ir.toSQLString()
      val s4 = ir.toSQLString()
      val s5 = ir.toSQLString()
      assertEquals(s1, s2)
      assertEquals(s1, s3)
      assertEquals(s1, s4)
      assertEquals(s1, s5)
      assert(s1.toLowerCase().contains("length("), s1)
    }
    {
      import tyql.Dialect.mysql.given
      // q = t.map(_ => tyql.Expr.StringLit("abc").charLength).toQueryIR // we do not regenerate
      val ir = q.toQueryIR
      val ir2 = q.toQueryIR
      val ir3 = q.toQueryIR
      val s1 = ir.toSQLString()
      val s2 = ir.toSQLString()
      val s3 = ir.toSQLString()
      val s4 = ir.toSQLString()
      val s5 = ir.toSQLString()
      assertEquals(s1, s2)
      assertEquals(s1, s3)
      assertEquals(s1, s4)
      assertEquals(s1, s5)
      assert(s1.toLowerCase().contains("char_length("), s1)
    }
  }
}
