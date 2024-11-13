package test.integration.datetimetests

import munit.FunSuite
import test.{withDBNoImplicits, withDB, checkExpr, checkExprDialect}
import java.sql.{Connection, Statement, ResultSet}
import tyql._

class DateTimeTests extends FunSuite {
  test("datetime extractions") {
    withDB.all { conn =>
      val db = new DB(conn)
      val got = db.run(Exprs[(y: Int, m: Int, d: Int, h: Int, mi: Int, s: Int)]((
        lit(java.time.LocalDateTime.parse("2023-08-15T14:30:00")).year,
        lit(java.time.LocalDateTime.parse("2023-08-15T14:30:00")).month,
        lit(java.time.LocalDateTime.parse("2023-08-15T14:30:00")).day,
        lit(java.time.LocalDateTime.parse("2023-08-15T14:30:00")).hour,
        lit(java.time.LocalDateTime.parse("2023-08-15T14:30:00")).minute,
        lit(java.time.LocalDateTime.parse("2023-08-15T14:30:00")).second
      )))
      assertEquals(got, List((2023, 8, 15, 14, 30, 0)))
    }
  }
  test("date extractions") {
    withDB.all { conn =>
      val db = new DB(conn)
      val got = db.run(Exprs[(y: Int, m: Int, d: Int)]((
        lit(java.time.LocalDate.parse("2023-08-15")).year,
        lit(java.time.LocalDate.parse("2023-08-15")).month,
        lit(java.time.LocalDate.parse("2023-08-15")).day
      )))
      assertEquals(got, List((2023, 8, 15)))
    }
  }
  test("current date and time") {
    withDB.all { conn =>
      val db = new DB(conn)
      val got = db.run(Exprs[(y: Int, y2: Int)]((
        CurrentTime.year,
        CurrentDate.year
      )))
      assert(got.length == 1)
      assert(got.head.toList(0) >= 2025)
      assert(got.head.toList(1) >= 2025)
    }
  }
}
