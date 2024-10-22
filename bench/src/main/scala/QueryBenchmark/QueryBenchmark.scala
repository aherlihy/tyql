package tyql.bench

import scala.annotation.experimental

@experimental
trait QueryBenchmark {
  def initializeCollections(): Unit
  def executeTyQL(ddb: DuckDBBackend): Unit
  def executeScalaSQL(ddb: DuckDBBackend): Unit
  def executeCollections(): Unit
  def writeTyQLResult(): Unit
  def writeCollectionsResult(): Unit
  def writeScalaSQLResult(): Unit
}
