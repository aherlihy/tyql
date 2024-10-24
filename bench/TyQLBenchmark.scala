package tyql.bench

import org.openjdk.jmh.annotations.*
import org.openjdk.jmh.infra.Blackhole
import java.util.concurrent.TimeUnit
import scala.annotation.experimental
import Helpers.*

@experimental
@Fork(1)
@Warmup(iterations = 1, time = 1, timeUnit = TimeUnit.SECONDS, batchSize = 1)
@Measurement(iterations = 1, time = 1, timeUnit = TimeUnit.SECONDS, batchSize= 1)
@State(Scope.Thread)
@BenchmarkMode(Array(Mode.AverageTime))
class TyQLBenchmark {
  var duckDB = DuckDBBackend()
  val benchmarks = Map(
    "tc" -> TCQuery(),
    "sssp" -> SSSPQuery(),
    "ancestry" -> AncestryQuery(),
    "andersens" -> AndersensQuery(),
    "asps" -> ASPSQuery(),
    "bom" -> BOMQuery(),
    "orbits" -> OrbitsQuery(),
    "dataflow" -> DataflowQuery(),
    "evenodd" -> EvenOddQuery(),
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
      bm.writeTyQLResult()
    )
    duckDB.close()
  }

  /*******************Boilerplate*****************/
  @Benchmark def tc(blackhole: Blackhole): Unit = {
    blackhole.consume(
      benchmarks("tc").executeTyQL(duckDB)
    )
  }

  @Benchmark def sssp(blackhole: Blackhole): Unit = {
    blackhole.consume(
      benchmarks("sssp").executeTyQL(duckDB)
    )
  }

  @Benchmark def ancestry(blackhole: Blackhole): Unit = {
    blackhole.consume(
      benchmarks("ancestry").executeTyQL(duckDB)
    )
  }

  @Benchmark def andersens(blackhole: Blackhole): Unit = {
    blackhole.consume(
      benchmarks("andersens").executeTyQL(duckDB)
    )
  }

  @Benchmark def asps(blackhole: Blackhole): Unit = {
    blackhole.consume(
      benchmarks("asps").executeTyQL(duckDB)
    )
  }

  @Benchmark def bom(blackhole: Blackhole): Unit = {
    blackhole.consume(
      benchmarks("bom").executeTyQL(duckDB)
    )
  }

  @Benchmark def orbits(blackhole: Blackhole): Unit = {
    blackhole.consume(
      benchmarks("orbits").executeTyQL(duckDB)
    )
  }

  @Benchmark def dataflow(blackhole: Blackhole): Unit = {
    blackhole.consume(
      benchmarks("dataflow").executeTyQL(duckDB)
    )
  }

  @Benchmark def evenodd(blackhole: Blackhole): Unit = {
    blackhole.consume(
      benchmarks("evenodd").executeTyQL(duckDB)
    )
  }
}
