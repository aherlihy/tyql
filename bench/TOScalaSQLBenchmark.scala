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
@Measurement(iterations = 1, time = 1, timeUnit = TimeUnit.MILLISECONDS, batchSize = 1)
@State(Scope.Thread)
@BenchmarkMode(Array(Mode.AverageTime))
class TOScalaSQLBenchmark {
  private def runWithTimeout(benchmarkName: String, blackhole: Blackhole): Unit = {
    val executor = Executors.newSingleThreadExecutor()
    val future: Future[Unit] = executor.submit(() => {
      blackhole.consume(benchmarks(benchmarkName).executeScalaSQL(duckDB))
    })

    try {
      // Await completion or timeout
      future.get(timeoutMins, TimeUnit.MINUTES)
    } catch {
      case e: InterruptedException => // New: Catch the interrupt signal
        println(s"Benchmark '$benchmarkName' was interrupted.")
        Thread.currentThread().interrupt()
      case e: Exception =>
        throw e
      //        println(s"Benchmark '$benchmarkName' encountered an exception: ${e.getMessage}")
    } finally {
      duckDB.cancelStmt()
      executor.shutdownNow() // Ensure cleanup
    }
  }

  var duckDB = DuckDBBackend(timeout = timeoutMins)
  val benchmarks = Map(
    // "tc" -> TOTCQuery(),
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

  def run(bm: String) = benchmarks(bm).executeScalaSQL(duckDB)

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
      bm.writeScalaSQLResult()
    )
    duckDB.close()
  }

  /** *****************Boilerplate****************
    */
  @Benchmark def tc_large(blackhole: Blackhole): Unit = {
    runWithTimeout("tc", blackhole)
  }

  @Benchmark def sssp_large(blackhole: Blackhole): Unit = {
    runWithTimeout("sssp", blackhole)
  }

  @Benchmark def ancestry_large(blackhole: Blackhole): Unit = {
    runWithTimeout("ancestry", blackhole)
  }

  @Benchmark def andersens_small(blackhole: Blackhole): Unit = {
    runWithTimeout("andersens", blackhole)
  }

  @Benchmark def asps_medium(blackhole: Blackhole): Unit = {
    runWithTimeout("asps", blackhole)
  }

  @Benchmark def bom_medium(blackhole: Blackhole): Unit = {
    runWithTimeout("bom", blackhole)
  }

  @Benchmark def orbits_medium(blackhole: Blackhole): Unit = {
    runWithTimeout("orbits", blackhole)
  }

  @Benchmark def dataflow_small(blackhole: Blackhole): Unit = {
    runWithTimeout("dataflow", blackhole)
  }

  @Benchmark def evenodd_medium(blackhole: Blackhole): Unit = {
    runWithTimeout("evenodd", blackhole)
  }

  @Benchmark def cc_medium(blackhole: Blackhole): Unit = {
    runWithTimeout("cc", blackhole)
  }

  @Benchmark def pointstocount_small(blackhole: Blackhole): Unit = {
    runWithTimeout("pointstocount", blackhole)
  }

  @Benchmark def javapointsto_small(blackhole: Blackhole): Unit = {
    runWithTimeout("javapointsto", blackhole)
  }

  @Benchmark def trustchain_medium(blackhole: Blackhole): Unit = {
    runWithTimeout("trustchain", blackhole)
  }

  @Benchmark def party_medium(blackhole: Blackhole): Unit = {
    runWithTimeout("party", blackhole)
  }

  @Benchmark def cspa_medium(blackhole: Blackhole): Unit = {
    runWithTimeout("cspa", blackhole)
  }

  @Benchmark def cba_small(blackhole: Blackhole): Unit = {
    runWithTimeout("cba", blackhole)
  }
}
