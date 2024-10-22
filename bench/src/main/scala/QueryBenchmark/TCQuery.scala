package tyql.bench

import java.sql.{Connection, ResultSet}
import scala.annotation.experimental

@experimental
class TCQuery extends QueryBenchmark {
  val name = "tc"

  def executeDuckDB(ddb: DuckDBBackend): Unit =
    val result = ddb.runQuery("SELECT * FROM tc_edge")
    extractResultJDBC(result)

  // Extract all results to avoid lazy-loading
  def extractResultJDBC(resultSet: ResultSet): Unit =
    println("Query Results:")
    while (resultSet.next()) {
      val x = resultSet.getInt("x")
      val y = resultSet.getInt("y")
      println(s"x: $x, y: $y")
    }

  def executeCollections(cdb: CollectionsBackend): Unit = ???
}
