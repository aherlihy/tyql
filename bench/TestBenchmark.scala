package tyql.benchmarks

import java.util.concurrent.TimeUnit
import org.openjdk.jmh.annotations.*
import org.openjdk.jmh.infra.Blackhole

import java.nio.file.{FileSystems, Files, Path, Paths}
import scala.util.Using
import scala.collection.immutable.Map
import scala.concurrent.duration.Duration

import scala.sys.process.Process

@Fork(1) // # of jvms that it will use
@Warmup(iterations = 10, time = 10, timeUnit = TimeUnit.SECONDS, batchSize = 1)
@Measurement(iterations = 10, time = 10, timeUnit = TimeUnit.SECONDS, batchSize= 1)
@State(Scope.Thread)
@BenchmarkMode(Array(Mode.AverageTime))
class TestBenchmark {
  @Setup(Level.Iteration)
  def s(): Unit = {}

  @Benchmark def test_benchmark(blackhole: Blackhole): Unit = {
    blackhole.consume(
      println("testing benchmark")
    )
  }
}
