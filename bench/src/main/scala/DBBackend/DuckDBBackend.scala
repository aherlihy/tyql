package tyql.bench

import buildinfo.BuildInfo

import java.sql.{Connection, DriverManager, ResultSet, Statement}
import scala.annotation.experimental
import Helpers.*
import scalasql.{DbClient, Config}
import scalasql.PostgresDialect._

@experimental
class DuckDBBackend(timeout: Int = -1) {
  var connection: Connection = null
  var scalaSqlDb: DbClient = null
  var lastStmt: Statement = null

  def connect(): Unit =
    Class.forName("org.duckdb.DuckDBDriver")

    connection = DriverManager.getConnection("jdbc:duckdb:")
    scalaSqlDb = new scalasql.DbClient.Connection(connection, new Config{
      override def nameMapper(v: String) = v
      override def tableNameMapper(v: String) = s"${v.toLowerCase()}"
      override def defaultQueryTimeoutSeconds: Int = timeout
    })
    val s = connection.createStatement()

//    s.execute("PRAGMA enable_profiling='json'")
//    s.execute("PRAGMA profiling_output='profiling_output.json'")


  def dropData(benchmark: String): Unit =
    val datadir = s"${BuildInfo.baseDirectory}/bench/data/$benchmark/"
    val ddl = s"${datadir}/drop_tmp.ddl"
    val ddlCmds = readDDLFile(ddl)
    val statement = connection.createStatement()

    ddlCmds.foreach(ddl =>
//      println(s"Executing DDL: $ddl")
      statement.execute(ddl)
    )

  def loadData(benchmark: String): Unit =
    val datadir = s"${BuildInfo.baseDirectory}/bench/data/$benchmark/"

    val ddl = s"${datadir}/schema.ddl"
    val ddlCmds = readDDLFile(ddl)
    val statement = connection.createStatement()

    ddlCmds.foreach(ddl =>
//      println(s"Executing DDL: $ddl")
      statement.execute(ddl)
    )

    val allCSV = getCSVFiles(datadir)
    allCSV.foreach(csv =>
      val table = csv.getFileName().toString.replace(".csv", "")
      statement.execute(s"COPY ${benchmark}_$table FROM '$csv'")
      // print ok:
      val checkQ = statement.executeQuery(s"SELECT COUNT(*) FROM ${benchmark}_$table")
      checkQ.next()
//      println(s"LOADED into ${benchmark}_$table: ${checkQ.getInt(1)}")
    )

  def runQuery(sqlString: String): ResultSet =
    lastStmt = connection.createStatement()
    lastStmt.setQueryTimeout(timeout)
    lastStmt.executeQuery(sqlString)

  def runUpdate(sqlString: String): Unit =
    lastStmt = connection.createStatement()
    lastStmt.setQueryTimeout(timeout)
    lastStmt.executeUpdate(sqlString)

  def cancelStmt(): Unit =
    try {
      if lastStmt != null then lastStmt.cancel()
    } catch {
      case e: Exception => println(s"Error cancelling statement: $e")
    }

  def close(): Unit =
    connection.close()
//    scalaSqlDb.close()

  def printTables(): Unit = {
    println("Current Tables")
    val statement = connection.createStatement()
    val query = "SELECT table_name, estimated_size FROM duckdb_tables();"

    // Execute the query and print results
    val resultSet: ResultSet = statement.executeQuery(query)
    var tables = Seq[(String, String)]()
    while resultSet.next() do
      val tableName = resultSet.getString("table_name")
      val size = resultSet.getLong("estimated_size")
      tables = tables :+ (tableName, size.toString)

    tables.filterNot(t => (t._1.contains("delta") || t._1.contains("derived") || t._1.contains("tmp"))).foreach(t => println(s"${t._1}\t\t\t${t._2}"))
    tables.filter(t => (t._1.contains("delta") || t._1.contains("derived") || t._1.contains("tmp"))).foreach(t => println(s"\t${t._1}\t\t${t._2}"))
    // Clean up resources
    resultSet.close()
    statement.close()
  }
}