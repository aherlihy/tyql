package tyql.bench

import buildinfo.BuildInfo

import scala.annotation.experimental
import scalasql.DbClient

@experimental
trait QueryBenchmark {
  def name: String
  def datadir = s"${BuildInfo.baseDirectory}/bench/data/$name"
  def outdir = s"$datadir/out"

  def initializeCollections(): Unit
  def executeTyQL(ddb: DuckDBBackend): Unit
  def executeScalaSQL(dbClient: DbClient): Unit
  def executeCollections(): Unit
  def writeTyQLResult(): Unit
  def writeCollectionsResult(): Unit
  def writeScalaSQLResult(): Unit
}
