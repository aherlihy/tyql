package tyql.bench

import buildinfo.BuildInfo

import java.sql.{Connection, DriverManager, ResultSet, Statement}
import scala.annotation.experimental
import Helpers.*
import scalasql.{DbClient, Config}
import scalasql.PostgresDialect._

@experimental
class DuckDBBackend {
  var connection: Connection = null
  var scalaSqlDb: DbClient = null

  def connect(): Unit =
    Class.forName("org.duckdb.DuckDBDriver")

    connection = DriverManager.getConnection("jdbc:duckdb:")
    scalaSqlDb = new scalasql.DbClient.Connection(connection, new Config{
      override def nameMapper(v: String) = v
      override def tableNameMapper(v: String) = s"${v.toLowerCase()}"
    })

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
    val statement = connection.createStatement()
    statement.executeQuery(sqlString)

  def close(): Unit =
    connection.close()
//    scalaSqlDb.close()
}