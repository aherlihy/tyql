package tyql

// I profiled query generation with Async Profiler and I have not noticed anything standing out, it probably does not make sense to
// optimize this further, but we should verify that string generation takes more time than query generation, since benchmarks with
// JHM tell me that query generation vs string generation is 60 vs 40% and Async Profiler tells me that string generation is 60 vs 40%.
// To use Async Profiler I had to, on my machine, do
// sudo sysctl kernel.kptr_restrict=0  # from 0
// sudo sysctl kernel.perf_event_paranoid=1  # from 4
class SQLRenderingContext {
  val sql = new StringBuilder
  val parameters = new scala.collection.mutable.ArrayBuffer[Object]

  def mkString(elements: Seq[QueryIRNode], sep: String)(using d: Dialect)(using cnf: Config): Unit =
    mkString(elements, "", sep, "")(using d)(using cnf)

  def mkString
    (elements: Seq[QueryIRNode], start: String, sep: String, end: String)
    (using d: Dialect)
    (using cnf: Config)
    : Unit =
    sql.append(start)
    var first = true
    for e <- elements do
      if first then
        first = false
      else
        sql.append(sep)
      e.computeSQL(this)
    sql.append(end)
}
