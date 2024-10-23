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
class CollectionsBenchmark {
  val benchmarks = Map(
    "tc" -> TCQuery(),
    "sssp" -> SSSPQuery()
  )

  @Setup(Level.Trial)
  def loadDB(): Unit = {
    benchmarks.values.foreach(bm =>
      bm.initializeCollections()
      deleteOutputFiles(bm.outdir, "collections")
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
    blackhole.consume(
      benchmarks("tc").executeCollections()
    )
  }

  @Benchmark def sssp(blackhole: Blackhole): Unit = {
    blackhole.consume(
      benchmarks("sssp").executeCollections()
    )
  }
}
