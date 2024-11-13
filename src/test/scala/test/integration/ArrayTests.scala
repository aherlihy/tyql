package test.integration.arrays

import munit.FunSuite
import test.{withDBNoImplicits, withDB, checkExpr, checkExprDialect}
import java.sql.{Connection, Statement, ResultSet}
import tyql._
import tyql.Dialect.mysql.given
import scala.language.implicitConversions

class ArrayTest extends FunSuite {
  test("literal arrays") {
    withDB.allWithArraySupport { conn =>
      val db = tyql.DB(conn)
      val q = Exprs[(a: List[String])](Tuple1(List("a", "b", "c")))
      val got = db.run(q)
      assertEquals(got.length, 1)
      assertEquals(
        got(0).toList(0),
        List("a", "b", "c")
      ) // workaround around using got.head.a which triggers some bug in the compiler
    }

    withDB.allWithArraySupport { conn =>
      val db = tyql.DB(conn)
      val q = Exprs[(a: List[Long])](Tuple1(List(1L, 2L, 5L)))
      val got = db.run(q)
      assertEquals(got.length, 1)
      assertEquals(
        got(0).toList(0),
        List(1L, 2L, 5L)
      ) // workaround around using got.head.a which triggers some bug in the compiler
    }

    withDB.allWithArraySupport { conn =>
      val db = tyql.DB(conn)
      val q = Exprs[(a: List[Boolean])](Tuple1(tyql.Expr.ListExpr[Boolean](List(lit(true), lit(false), lit(false)))))
      val got = db.run(q)
      assertEquals(got.length, 1)
      assertEquals(
        got(0).toList(0),
        List(true, false, false)
      ) // workaround around using got.head.a which triggers some bug in the compiler
    }
  }

  test("array length") {
    withDB.allWithArraySupport { conn =>
      val db = tyql.DB(conn)
      val q = Exprs[(a: Int)](Tuple1(lit(List("a", "b", "c", "e")).length))
      val got = db.run(q)
      assertEquals(got.length, 1)
      assertEquals(got.head.toList.head, 4)
    }
  }

  test("array indexing") {
    withDB.allWithArraySupport { conn =>
      val db = tyql.DB(conn)
      val q = Exprs[(a: String)](Tuple1(lit(List("a", "b", "c", "e"))(0)))
      val got = db.run(q)
      assertEquals(got.length, 1)
      assertEquals(got.head.toList.head, "a")
    }

    withDB.allWithArraySupport { conn =>
      val db = tyql.DB(conn)
      val q = Exprs[(a: String)](Tuple1(lit(List("a", "b", "c", "e"))(2)))
      val got = db.run(q)
      assertEquals(got.length, 1)
      assertEquals(got.head.toList.head, "c")
    }

    withDB.allWithArraySupport { conn =>
      val db = tyql.DB(conn)
      val q = Exprs[(a: String)](Tuple1(lit(List("a", "b", "c", "e"))(3)))
      val got = db.run(q)
      assertEquals(got.length, 1)
      assertEquals(got.head.toList.head, "e")
    }
  }

  test("array concat") {
    withDB.allWithArraySupport { conn =>
      val db = tyql.DB(conn)
      val q = Exprs[(a: List[String])](Tuple1(lit(List("a", "b", "c", "e")) ++ lit(List("f", "g", "h"))))
      val got = db.run(q)
      assertEquals(got.length, 1)
      assertEquals(got.head.toList.head, List("a", "b", "c", "e", "f", "g", "h"))
    }
  }

  test("array prepend") {
    withDB.allWithArraySupport { conn =>
      val db = tyql.DB(conn)
      val q = Exprs[(a: List[String])](Tuple1(lit(List("a", "b", "c", "e")).prepend("X")))
      val got = db.run(q)
      assertEquals(got.length, 1)
      assertEquals(got.head.toList.head, List("X", "a", "b", "c", "e"))
    }
  }

  test("array append") {
    withDB.allWithArraySupport { conn =>
      val db = tyql.DB(conn)
      val q = Exprs[(a: List[String])](Tuple1(lit(List("a", "b", "c", "e")).append("X")))
      val got = db.run(q)
      assertEquals(got.length, 1)
      assertEquals(got.head.toList.head, List("a", "b", "c", "e", "X"))
    }
  }

  test("array contains true") {
    withDB.allWithArraySupport { conn =>
      val db = tyql.DB(conn)
      val q = Exprs[(a: Boolean)](Tuple1(lit(List("a", "b", "c", "e")).containsElement("a")))
      val got = db.run(q)
      assertEquals(got.length, 1)
      assertEquals(got.head.toList.head, true)
    }
  }

  test("array contains false") {
    withDB.allWithArraySupport { conn =>
      val db = tyql.DB(conn)
      val q = Exprs[(a: Boolean)](Tuple1(lit(List("a", "b", "c", "e")).containsElement("X")))
      val got = db.run(q)
      assertEquals(got.length, 1)
      assertEquals(got.head.toList.head, false)
    }
  }
}
