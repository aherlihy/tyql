package tyql.bench

import org.openjdk.jmh.annotations.*
import org.openjdk.jmh.infra.Blackhole
import java.util.concurrent.TimeUnit
import scala.annotation.experimental
import Helpers.*

@experimental
@Fork(1)
@Warmup(iterations = 0, time = 1, timeUnit = TimeUnit.MILLISECONDS, batchSize = 1)
@Measurement(iterations = 1, time = 1, timeUnit = TimeUnit.MILLISECONDS, batchSize= 1)
@State(Scope.Thread)
@BenchmarkMode(Array(Mode.AverageTime))
class ScalaSQLBenchmark {
  var duckDB = DuckDBBackend()
  val benchmarks = Map(
//    "tc" -> TCQuery(),
//    "sssp" -> SSSPQuery(),
    "ancestry" -> AncestryQuery(),
//    "andersens" -> AndersensQuery(),
//    "asps" -> ASPSQuery(),
//    "bom" -> BOMQuery(),
//    "orbits" -> OrbitsQuery(),
//    "dataflow" -> DataflowQuery(),
//    "evenodd" -> EvenOddQuery(),
//    "cc" -> CompanyControlQuery(),
//    "pointstocount" -> PointsToCountQuery(),
//    "javapointsto" -> JavaPointsTo(),
//    "trustchain" -> TrustChainQuery(),
//    "party" -> PartyQuery(),
//    "cspa" -> CSPAQuery(),
//    "cba" -> CBAQuery(),
  )

  def run(bm: String) = benchmarks(bm).executeScalaSQL(duckDB)

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

  @Benchmark def pointstocount(blackhole: Blackhole): Unit = {
    blackhole.consume(
      run("pointstocount")
    )
  }

  @Benchmark def javapointsto(blackhole: Blackhole): Unit = {
    blackhole.consume(
      run("javapointsto")
    )
  }

  @Benchmark def trustchain(blackhole: Blackhole): Unit = {
    blackhole.consume(
      run("trustchain")
    )
  }

  @Benchmark def party(blackhole: Blackhole): Unit = {
    blackhole.consume(
      run("party")
    )
  }

  @Benchmark def cspa(blackhole: Blackhole): Unit = {
    blackhole.consume(
      run("cspa")
    )
  }

  @Benchmark def cba(blackhole: Blackhole): Unit = {
    blackhole.consume(
      run("cba")
    )
  }
}
