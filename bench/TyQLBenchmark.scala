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
    "cc" -> CompanyControlQuery(),
  )

  def run(bm: String) = benchmarks(bm).executeTyQL(duckDB)

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
      run("tc")
    )
  }

  @Benchmark def sssp(blackhole: Blackhole): Unit = {
    blackhole.consume(
      run("sssp")
    )
  }

  @Benchmark def ancestry(blackhole: Blackhole): Unit = {
    blackhole.consume(
      run("ancestry")
    )
  }

  @Benchmark def andersens(blackhole: Blackhole): Unit = {
    blackhole.consume(
      run("andersens")
    )
  }

  @Benchmark def asps(blackhole: Blackhole): Unit = {
    blackhole.consume(
      run("asps")
    )
  }

  @Benchmark def bom(blackhole: Blackhole): Unit = {
    blackhole.consume(
      run("bom")
    )
  }

  @Benchmark def orbits(blackhole: Blackhole): Unit = {
    blackhole.consume(
      run("orbits")
    )
  }

  @Benchmark def dataflow(blackhole: Blackhole): Unit = {
    blackhole.consume(
      run("dataflow")
    )
  }

  @Benchmark def evenodd(blackhole: Blackhole): Unit = {
    blackhole.consume(
      run("evenodd")
    )
  }

  @Benchmark def cc(blackhole: Blackhole): Unit = {
    blackhole.consume(
      run("cc")
    )
  }
}
