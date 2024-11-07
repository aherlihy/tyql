package tyql.bench

import org.openjdk.jmh.annotations.*
import org.openjdk.jmh.infra.Blackhole
import java.util.concurrent.TimeUnit
import scala.annotation.experimental
import java.util.concurrent.{Executors, TimeUnit, TimeoutException, Future}

import Helpers.*

@experimental
@Fork(1)
@Warmup(iterations = 0, time = 1, timeUnit = TimeUnit.MILLISECONDS, batchSize = 1)
@Measurement(iterations = 1, time = 1, timeUnit = TimeUnit.MILLISECONDS, batchSize= 1)
@State(Scope.Thread)
@BenchmarkMode(Array(Mode.AverageTime))
class TOCollectionsBenchmark {
  val timeoutMillis = 300000
  private def runWithTimeout(benchmarkName: String, blackhole: Blackhole): Unit = {
    val executor = Executors.newSingleThreadExecutor()
    val future: Future[Unit] = executor.submit(() => {
      blackhole.consume(benchmarks(benchmarkName).executeCollections())
    })

    try {
      // Await completion or timeout
      future.get(timeoutMillis, TimeUnit.MILLISECONDS)
    } catch {
      case e: InterruptedException => // New: Catch the interrupt signal
        println(s"Benchmark '$benchmarkName' was interrupted.")
        Thread.currentThread().interrupt()
      case e: Exception =>
        throw e
//        println(s"Benchmark '$benchmarkName' encountered an exception: ${e.getMessage}")
    } finally {
      executor.shutdownNow() // Ensure cleanup
    }
  }

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

  @Setup(Level.Trial)
  def loadDB(): Unit = {
    benchmarks.values.foreach(bm =>
      if !Helpers.skip.contains(bm.name) then bm.initializeCollections()
    )
  }

  @TearDown(Level.Trial)
  def writeDB(): Unit = {
    benchmarks.values.foreach(bm =>
      bm.writeCollectionsResult()
    )
  }

  /*******************Boilerplate*****************/
  @Benchmark def tc(blackhole: Blackhole): Unit = {
    runWithTimeout("tc", blackhole)
  }

  @Benchmark def sssp(blackhole: Blackhole): Unit = {
    runWithTimeout("sssp", blackhole)
  }

  @Benchmark def ancestry(blackhole: Blackhole): Unit = {
    runWithTimeout("ancestry", blackhole)
  }

  @Benchmark def andersens(blackhole: Blackhole): Unit = {
    runWithTimeout("andersens", blackhole)
  }

  @Benchmark def asps(blackhole: Blackhole): Unit = {
    runWithTimeout("asps", blackhole)
  }

  @Benchmark def bom(blackhole: Blackhole): Unit = {
    runWithTimeout("bom", blackhole)
  }

  @Benchmark def orbits(blackhole: Blackhole): Unit = {
    runWithTimeout("orbits", blackhole)
  }

  @Benchmark def dataflow(blackhole: Blackhole): Unit = {
    runWithTimeout("dataflow", blackhole)
  }

  @Benchmark def evenodd(blackhole: Blackhole): Unit = {
    runWithTimeout("evenodd", blackhole)
  }

  @Benchmark def cc(blackhole: Blackhole): Unit = {
    runWithTimeout("cc", blackhole)
  }

  @Benchmark def pointstocount(blackhole: Blackhole): Unit = {
    runWithTimeout("pointstocount", blackhole)
  }

  @Benchmark def javapointsto(blackhole: Blackhole): Unit = {
    runWithTimeout("javapointsto", blackhole)
  }

  @Benchmark def trustchain(blackhole: Blackhole): Unit = {
    runWithTimeout("trustchain", blackhole)
  }

  @Benchmark def party(blackhole: Blackhole): Unit = {
    runWithTimeout("party", blackhole)
  }

  @Benchmark def cspa(blackhole: Blackhole): Unit = {
    runWithTimeout("cspa", blackhole)
  }

  @Benchmark def cba(blackhole: Blackhole): Unit = {
    runWithTimeout("cba", blackhole)
  }
}
