package tyql.bench

import buildinfo.BuildInfo

import java.sql.{Connection, DriverManager, ResultSet, Statement}
import scala.annotation.experimental
import Helpers.*

@experimental
class DuckDBBackend {
  var connection: Connection = null

  def connect(): Unit =
    Class.forName("org.duckdb.DuckDBDriver")

    connection = DriverManager.getConnection("jdbc:duckdb:")

  def loadData(benchmark: String): Unit =
    val datadir = s"${BuildInfo.baseDirectory}/bench/data/$benchmark/"

    val ddl = s"${datadir}/schema.ddl"
    val ddlCmds = readDDLFile(ddl)
    val statement = connection.createStatement()

    ddlCmds.foreach(ddl =>
      println(s"Executing DDL: $ddl")
      statement.execute(ddl)
    )


    val allCSV = getCSVFiles(datadir)
    allCSV.foreach(csv =>
      statement.execute(s"COPY ${benchmark}_edge FROM '$csv'")
    )



  def runQuery(sqlString: String): ResultSet =
    val statement = connection.createStatement()
    statement.executeQuery(sqlString)

  def close(): Unit =
    connection.close()
}