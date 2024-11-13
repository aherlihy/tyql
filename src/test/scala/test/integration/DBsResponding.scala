package test.integration.dbsresponding

import munit.FunSuite
import java.sql.{Connection, DriverManager}

class DBsResponding extends FunSuite {
  def withConnection[A](url: String, user: String = "", password: String = "")(f: Connection => A): A = {
    var conn: Connection = null
    try {
      conn = DriverManager.getConnection(url, user, password)
      f(conn)
    } finally {
      if (conn != null) conn.close()
    }
  }

  test("PostgreSQL responds") {
    withConnection(
      "jdbc:postgresql://localhost:5433/testdb",
      "testuser",
      "testpass"
    ) { conn =>
      val stmt = conn.createStatement()
      val rs = stmt.executeQuery("SELECT ARRAY[5,10,3]::integer[] as arr")
      assert(rs.next())
      val arr = rs.getArray("arr").getArray().asInstanceOf[Array[Integer]]
      assertEquals(arr.toList.map(_.toInt), List(5, 10, 3))
    }
  }

  test("MySQL responds") {
    withConnection(
      "jdbc:mysql://localhost:3307/testdb",
      "testuser",
      "testpass"
    ) { conn =>
      val stmt = conn.createStatement()
      val rs = stmt.executeQuery(
        """SELECT GROUP_CONCAT(n ORDER BY n SEPARATOR '-') as concat
           FROM (SELECT 7 as n UNION SELECT 22 UNION SELECT 31) nums"""
      )
      assert(rs.next())
      assertEquals(rs.getString("concat"), "7-22-31")
    }
  }

  test("MariaDB responds") {
    withConnection(
      "jdbc:mariadb://localhost:3308/testdb",
      "testuser",
      "testpass"
    ) { conn =>
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

  test("SQLite responds with its unique json_each table-valued function") {
    withConnection("jdbc:sqlite::memory:") { conn =>
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

  test("DuckDB responds") {
    withConnection("jdbc:duckdb:") { conn =>
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
}
