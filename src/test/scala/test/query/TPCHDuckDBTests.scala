package test.query.tpch

import java.sql.{Connection, DriverManager, ResultSet}
import scala.annotation.experimental

@experimental
class TPCHDuckDBTests extends munit.FunSuite {
  var connection: Connection = null

  override def beforeAll(): Unit = {
    Class.forName("org.duckdb.DuckDBDriver")
    connection = DriverManager.getConnection("jdbc:duckdb:")

    val stmt = connection.createStatement()
    stmt.execute("INSTALL tpch")
    stmt.execute("LOAD tpch")
    stmt.execute("CALL dbgen(sf=0.01)")
    stmt.close()
  }

  override def afterAll(): Unit = {
    if (connection != null) connection.close()
  }

  private def executeQuery(sql: String): ResultSet = {
    val stmt = connection.createStatement()
    stmt.executeQuery(sql)
  }

  private def getGeneratedSQL(q: tyql.DatabaseAST[?]): String = {
    q.toQueryIR.toSQLString().trim
  }

  test("TPCH Q1 executes on DuckDB") {
    val q = new TPCHQ1Test
    val sql = getGeneratedSQL(q.query())
    val rs = executeQuery(sql)
    val meta = rs.getMetaData
    assert(meta.getColumnCount == 10, s"Expected 10 columns, got ${meta.getColumnCount}")
    var rows = 0
    while (rs.next()) { rows += 1 }
    assert(rows > 0, "Q1 should return at least one row")
  }

  test("TPCH Q3 executes on DuckDB") {
    val q = new TPCHQ3Test
    val sql = getGeneratedSQL(q.query())
    val rs = executeQuery(sql)
    val meta = rs.getMetaData
    assert(meta.getColumnCount == 4, s"Expected 4 columns, got ${meta.getColumnCount}")
  }

  test("TPCH Q4 executes on DuckDB") {
    val q = new TPCHQ4Test
    val sql = getGeneratedSQL(q.query())
    val rs = executeQuery(sql)
    val meta = rs.getMetaData
    assert(meta.getColumnCount == 2, s"Expected 2 columns, got ${meta.getColumnCount}")
  }

  test("TPCH Q6 executes on DuckDB") {
    val q = new TPCHQ6Test
    val sql = getGeneratedSQL(q.query())
    val rs = executeQuery(sql)
    val meta = rs.getMetaData
    assert(meta.getColumnCount == 1, s"Expected 1 column, got ${meta.getColumnCount}")
    assert(rs.next(), "Q6 should return one row")
  }

  test("TPCH Q10 executes on DuckDB") {
    val q = new TPCHQ10Test
    val sql = getGeneratedSQL(q.query())
    val rs = executeQuery(sql)
    val meta = rs.getMetaData
    assert(meta.getColumnCount == 8, s"Expected 8 columns, got ${meta.getColumnCount}")
  }

  test("TPCH Q11 executes on DuckDB") {
    val q = new TPCHQ11Test
    val sql = getGeneratedSQL(q.query())
    val rs = executeQuery(sql)
    val meta = rs.getMetaData
    assert(meta.getColumnCount == 2, s"Expected 2 columns, got ${meta.getColumnCount}")
  }
}
