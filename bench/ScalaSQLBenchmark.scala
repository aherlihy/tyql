package tyql.bench

import org.openjdk.jmh.annotations.*
import org.openjdk.jmh.infra.Blackhole
import java.util.concurrent.TimeUnit
import scala.annotation.experimental
import Helpers.*

@experimental
@Fork(1)
@Warmup(iterations = 1, time = 1, timeUnit = TimeUnit.MILLISECONDS, batchSize = 1)
@Measurement(iterations = 1, time = 1, timeUnit = TimeUnit.MILLISECONDS, batchSize= 1)
@State(Scope.Thread)
@BenchmarkMode(Array(Mode.AverageTime))
class ScalaSQLBenchmark {
  var duckDB = DuckDBBackend()
  val benchmarks = Map(
    "tc" -> TCQuery(),
    "sssp" -> SSSPQuery(),
    "ancestry" -> AncestryQuery(),
    "andersens" -> AndersensQuery(),
    "asps" -> ASPSQuery()
  )

  @Setup(Level.Trial)
  def loadDB(): Unit = {
    duckDB.connect()
    benchmarks.values.foreach(bm =>
      duckDB.loadData(bm.name)
    )
  }

  @TearDown(Level.Trial)
  def close(): Unit = {
    benchmarks.values.foreach(bm =>
      bm.writeScalaSQLResult()
    )
    duckDB.close()
  }

  /*******************Boilerplate*****************/
  @Benchmark def tc(blackhole: Blackhole): Unit = {
    blackhole.consume(
      benchmarks("tc").executeScalaSQL(duckDB)
    )
  }

  @Benchmark def sssp(blackhole: Blackhole): Unit = {
    blackhole.consume(
      benchmarks("sssp").executeScalaSQL(duckDB)
    )
  }

  @Benchmark def ancestry(blackhole: Blackhole): Unit = {
    blackhole.consume(
      benchmarks("ancestry").executeScalaSQL(duckDB)
    )
  }
  @Benchmark def andersens(blackhole: Blackhole): Unit = {
    blackhole.consume(
      benchmarks("andersens").executeScalaSQL(duckDB)
    )
  }
  @Benchmark def asps(blackhole: Blackhole): Unit = {
    blackhole.consume(
      benchmarks("asps").executeScalaSQL(duckDB)
    )
  }
}
