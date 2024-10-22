package tyql.bench

import org.openjdk.jmh.annotations.*
import org.openjdk.jmh.infra.Blackhole
import java.util.concurrent.TimeUnit
import scala.annotation.experimental

@experimental
@Fork(1)
@Warmup(iterations = 1, time = 1, timeUnit = TimeUnit.SECONDS, batchSize = 1)
@Measurement(iterations = 1, time = 1, timeUnit = TimeUnit.SECONDS, batchSize= 1)
@State(Scope.Thread)
@BenchmarkMode(Array(Mode.AverageTime))
class RecursiveBenchmark {
  // interface to DB
  var collectionsDB = CollectionsBackend()
  var duckDB = DuckDBBackend()
  // benchmarks
  val benchmarks = Map(
    "tc" -> TCQuery()
  )

  /*******************Boilerplate*****************/
  @Setup(Level.Trial)
  def loadDB(): Unit = {
    duckDB.connect()
    benchmarks.values.foreach(bm =>
      duckDB.loadData(bm.name)
//      collectionsDB.loadData(bm.name)
    )
  }

//  @Benchmark def collections(blackhole: Blackhole): Unit = {
//    runCollectionsFix()
//    blackhole.consume(
//      println("collections")
//    )
//  }

  @Benchmark def tc_scalasql(blackhole: Blackhole): Unit = {
    blackhole.consume(
      benchmarks("tc").executeDuckDB(duckDB)
    )
  }

//  @Benchmark def recursive(blackhole: Blackhole): Unit = {
//    runRecursive()
//    blackhole.consume(
//      println("tyql")
//    )
//  }
}
