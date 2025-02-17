package tyql.bench

import org.openjdk.jmh.annotations.*
import org.openjdk.jmh.infra.Blackhole
import java.util.concurrent.TimeUnit
import java.util.concurrent.{Executors, TimeUnit, TimeoutException, Future}
import scala.annotation.experimental
import Helpers.*

@experimental
@Fork(1)
@Warmup(iterations = 0, time = 1, timeUnit = TimeUnit.MILLISECONDS, batchSize = 1)
@Measurement(iterations = 1, time = 1, timeUnit = TimeUnit.MILLISECONDS, batchSize= 1)
@State(Scope.Thread)
@BenchmarkMode(Array(Mode.AverageTime))
class TOTyQLBenchmark {
  private def runWithTimeout(benchmarkName: String, blackhole: Blackhole): Unit = {
    val executor = Executors.newSingleThreadExecutor()
    val future: Future[Unit] = executor.submit(() => {
      blackhole.consume(benchmarks(benchmarkName).executeTyQL(duckDB))
    })

    try {
      future.get(timeoutMins, TimeUnit.MINUTES)
    } catch {
      case e: InterruptedException => // New: Catch the interrupt signal
        println(s"Benchmark '$benchmarkName' was interrupted.")
        Thread.currentThread().interrupt()
      case e: Exception =>
        throw e
              println(s"Benchmark '$benchmarkName' encountered an exception: ${e.getMessage}")
    } finally {
      duckDB.cancelStmt()
      executor.shutdownNow() // Ensure cleanup
    }
  }

  var duckDB = DuckDBBackend(timeout = timeoutMins)

  val benchmarks = Map(
    "tc" -> TOTCQuery(),
    "sssp" -> TOSSSPQuery(),
    "ancestry" -> TOAncestryQuery(),
    "andersens" -> TOAndersensQuery(),
    "asps" -> TOASPSQuery(),
    "bom" -> TOBOMQuery(),
    "orbits" -> TOOrbitsQuery(),
    "dataflow" -> TODataflowQuery(),
    "evenodd" -> TOEvenOddQuery(),
    "cc" -> TOCompanyControlQuery(),
    "pointstocount" -> TOPointsToCountQuery(),
    "javapointsto" -> TOJavaPointsTo(),
    "trustchain" -> TOTrustChainQuery(),
    "party" -> TOPartyQuery(),
    "cspa" -> TOCSPAQuery(),
    "cba" -> TOCBAQuery(),
  )

  def run(bm: String) = benchmarks(bm).executeTyQL(duckDB)

  @Setup(Level.Trial)
  def loadDB(): Unit = {
    duckDB.connect()
    benchmarks.values.foreach(bm =>
      if !Helpers.skip.contains(bm.name) then duckDB.loadData(bm.name)
    )
  }

  @TearDown(Level.Trial)
  def close(): Unit = {
    benchmarks.values.foreach(bm =>
      bm.writeBenchResult(Helpers.QueryMode.TyQL)
    )
    duckDB.close()
  }

  /*******************Boilerplate*****************/
  @Benchmark def tc_graph(blackhole: Blackhole): Unit = {
    runWithTimeout("tc", blackhole)
  }

  @Benchmark def sssp_graph(blackhole: Blackhole): Unit = {
    runWithTimeout("sssp", blackhole)
  }

  @Benchmark def ancestry_graph(blackhole: Blackhole): Unit = {
    runWithTimeout("ancestry", blackhole)
  }

  @Benchmark def andersens_programanalysis(blackhole: Blackhole): Unit = {
    runWithTimeout("andersens", blackhole)
  }

  @Benchmark def asps_misc(blackhole: Blackhole): Unit = {
    runWithTimeout("asps", blackhole)
  }

  @Benchmark def bom_misc(blackhole: Blackhole): Unit = {
    runWithTimeout("bom", blackhole)
  }

  @Benchmark def orbits_misc(blackhole: Blackhole): Unit = {
    runWithTimeout("orbits", blackhole)
  }

  @Benchmark def dataflow_programanalysis(blackhole: Blackhole): Unit = {
    runWithTimeout("dataflow", blackhole)
  }

  @Benchmark def evenodd_misc(blackhole: Blackhole): Unit = {
    runWithTimeout("evenodd", blackhole)
  }

  @Benchmark def cc_misc(blackhole: Blackhole): Unit = {
    runWithTimeout("cc", blackhole)
  }

  @Benchmark def pointstocount_programanalysis(blackhole: Blackhole): Unit = {
    runWithTimeout("pointstocount", blackhole)
  }

  @Benchmark def javapointsto_programanalysis(blackhole: Blackhole): Unit = {
    runWithTimeout("javapointsto", blackhole)
  }

  @Benchmark def trustchain_misc(blackhole: Blackhole): Unit = {
    runWithTimeout("trustchain", blackhole)
  }

  @Benchmark def party_misc(blackhole: Blackhole): Unit = {
    runWithTimeout("party", blackhole)
  }

  @Benchmark def cspa_misc(blackhole: Blackhole): Unit = {
    runWithTimeout("cspa", blackhole)
  }

  @Benchmark def cba_programanalysis(blackhole: Blackhole): Unit = {
    runWithTimeout("cba", blackhole)
  }
}
