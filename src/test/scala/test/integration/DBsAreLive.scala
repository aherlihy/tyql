package test.integration.dbsresponding

import munit.FunSuite
import test.needsDBs

import java.sql.{Connection, DriverManager}
import test.withDB

class DBsAreLive extends FunSuite {
  test("PostgreSQL responds".tag(needsDBs)) {
    withDB.postgres { conn =>
      val stmt = conn.createStatement()
      val rs = stmt.executeQuery("SELECT ARRAY[5,10,3]::integer[] as arr")
      assert(rs.next())
      val arr = rs.getArray("arr").getArray().asInstanceOf[Array[Integer]]
      assertEquals(arr.toList.map(_.toInt), List(5, 10, 3))
    }
  }

  test("MySQL responds".tag(needsDBs)) {
    withDB.mysql { conn =>
      val stmt = conn.createStatement()
      val rs = stmt.executeQuery(
        """SELECT GROUP_CONCAT(n ORDER BY n SEPARATOR '-') as concat
           FROM (SELECT 7 as n UNION SELECT 22 UNION SELECT 31) nums"""
      )
      assert(rs.next())
      assertEquals(rs.getString("concat"), "7-22-31")
    }
  }

  test("MariaDB responds".tag(needsDBs)) {
    withDB.mariadb { conn =>
      val stmt = conn.createStatement()
      val rs = stmt.executeQuery(
        """SELECT seq FROM seq_1_to_4"""
      )
      var result = List.empty[Int]
      while (rs.next()) {
        result = result :+ rs.getInt("seq")
      }
      assertEquals(result, List(1, 2, 3, 4))
    }
  }

  test("SQLite responds".tag(needsDBs)) {
    withDB.sqlite { conn =>
      val stmt = conn.createStatement()
      val rs = stmt.executeQuery(
        """SELECT value FROM json_each('[5,55,3]') ORDER BY value"""
      )
      var result = List.empty[Int]
      while (rs.next()) {
        result = result :+ rs.getInt("value")
      }
      assertEquals(result, List(3, 5, 55))
    }
  }

  test("DuckDB responds".tag(needsDBs)) {
    withDB.duckdb { conn =>
      val stmt = conn.createStatement()
      val rs = stmt.executeQuery(
        """SELECT * FROM generate_series(1, 7, 2) as g"""
      )
      var result = List.empty[Int]
      while (rs.next()) {
        result = result :+ rs.getInt(1)
      }
      assertEquals(result, List(1, 3, 5, 7))
    }
  }

  test("H2 responds".tag(needsDBs)) {
    withDB.h2 { conn =>
      val stmt = conn.createStatement()

      val rs1 = stmt.executeQuery("""SELECT CASEWHEN(1=1, 'yes', 'no') as result""")
      assert(rs1.next())
      assertEquals(rs1.getString("result"), "yes")

      val rs2 = stmt.executeQuery("""SELECT DECODE(1, 1, 'one', 'other') as result""")
      assert(rs2.next())
      assertEquals(rs2.getString("result"), "one")
    }
  }
}
