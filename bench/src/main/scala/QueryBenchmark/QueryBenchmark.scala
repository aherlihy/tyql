package tyql.bench

import scala.annotation.experimental
import scalasql.DbClient

@experimental
trait QueryBenchmark {
  def initializeCollections(): Unit
  def executeTyQL(ddb: DuckDBBackend): Unit
  def executeScalaSQL(dbClient: DbClient): Unit
  def executeCollections(): Unit
  def writeTyQLResult(): Unit
  def writeCollectionsResult(): Unit
  def writeScalaSQLResult(): Unit
}
