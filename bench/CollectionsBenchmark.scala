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
    "sssp" -> SSSPQuery(),
    "ancestry" -> AncestryQuery(),
    "andersens" -> AndersensQuery(),
    "asps" -> ASPSQuery(),
    "bom" -> BOMQuery(),
    "orbits" -> OrbitsQuery(),
    "dataflow" -> DataflowQuery(),
    "evenodd" -> EvenOddQuery(),
  )
  benchmarks.values.foreach(bm =>
    bm.initializeCollections()
  )

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

  @Benchmark def ancestry(blackhole: Blackhole): Unit = {
    blackhole.consume(
      benchmarks("ancestry").executeCollections()
    )
  }

  @Benchmark def andersens(blackhole: Blackhole): Unit = {
    blackhole.consume(
      benchmarks("andersens").executeCollections()
    )
  }

  @Benchmark def asps(blackhole: Blackhole): Unit = {
    blackhole.consume(
      benchmarks("asps").executeCollections()
    )
  }

  @Benchmark def bom(blackhole: Blackhole): Unit = {
    blackhole.consume(
      benchmarks("bom").executeCollections()
    )
  }

  @Benchmark def orbits(blackhole: Blackhole): Unit = {
    blackhole.consume(
      benchmarks("orbits").executeCollections()
    )
  }

  @Benchmark def dataflow(blackhole: Blackhole): Unit = {
    blackhole.consume(
      benchmarks("dataflow").executeCollections()
    )
  }

  @Benchmark def evenodd(blackhole: Blackhole): Unit = {
    blackhole.consume(
      benchmarks("evenodd").executeCollections()
    )
  }
}
