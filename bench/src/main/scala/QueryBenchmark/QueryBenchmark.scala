package tyql.bench

import scala.annotation.experimental

trait QueryBenchmark {
  @experimental
  def executeDuckDB(ddb: DuckDBBackend): Unit
  def executeCollections(cdb: CollectionsBackend): Unit
}
