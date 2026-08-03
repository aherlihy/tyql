package test.query.composablefix

import tyql.*
import language.experimental.namedTuples
import NamedTuple.*
import scala.language.implicitConversions
import test.TestComparitor

/** Proof-of-concept tests for `composableFix`: the developer picks a backend by
 *  importing a dialect's givens and writes the recursive body with no profile;
 *  its actual shape is matched against the backend's capabilities. The same
 *  query source either compiles (and is specialized) or is a compile-time error,
 *  depending only on which dialect is imported. */
class ComposableFixTest extends munit.FunSuite:

  type Edge = (x: Int, y: Int)
  val Edges: Table[Edge] = Table[Edge]("edges")

  type BasicPart = (part: String, days: Int)
  type SubPart = (part: String, sub: String)
  val BasicParts: Table[BasicPart] = Table[BasicPart]("basicparts")
  val SubParts: Table[SubPart] = Table[SubPart]("subparts")

  type Parent = (par: String, ch: String)
  type Generation = (nm: String, g: Int)
  val Parents: Table[Parent] = Table[Parent]("parents")

  // ---------- positive: the backend supports what the query needs ----------

  // non-linear transitive closure: pathR is referenced twice -> needs keyed recursion
  test("DuckDB: non-linear transitive closure generates USING KEY"):
    import tyql.dialects.duckdb.given
    val q = Edges.composableFix(pathR =>
      pathR.flatMap(p =>
        pathR
          .filter(e => p.y == e.x)
          .map(e => (x = p.x, y = e.y)))
        .distinct)
    val sql = q.toSQLString
    println(s"[TC/DuckDB] $sql")
    assert(sql.contains("USING KEY"), sql)
    assert(sql.contains("recurring."), sql)

  // aggregation in the recursive step: needs recursive aggregation (DuckDB permits)
  test("DuckDB: recursive aggregation (bill-of-materials) compiles"):
    import tyql.dialects.duckdb.given
    import Expr.max
    val q = BasicParts.composableFix(waitFor =>
      SubParts.aggregate(sp =>
        waitFor
          .filter(wf => sp.sub == wf.part)
          .aggregate(wf => (part = sp.part, days = max(wf.days))))
        .groupBySource((wf, _) => (part = wf.part))
        .distinct)
    assert(q.toSQLString.contains("RECURSIVE"), q.toSQLString)

  // RaSQL: the SAME query source, but under the RaSQL dialect the aggregate is
  // declared in the recursive CTE head (endo-aggregate form), the GROUP BY is
  // dropped, and the body projects the raw column. This is the form RaSQL
  // (Gu et al. 2019) evaluates under the PreM property.
  test("RaSQL: recursive aggregation (bill-of-materials) declares the aggregate in the CTE head"):
    import tyql.dialects.rasql.given
    import Expr.max
    val q = BasicParts.composableFix(waitFor =>
      SubParts.aggregate(sp =>
        waitFor
          .filter(wf => sp.sub == wf.part)
          .aggregate(wf => (part = sp.part, days = max(wf.days))))
        .groupBySource((wf, _) => (part = wf.part))
        .distinct)
    val sql = q.toSQLString
    println(s"[BOM/RaSQL] $sql")
    // aggregate lifted into the head: `recursive$N(part, max() as days)`
    assert(sql.contains("(part, max() as days)"), s"expected in-head aggregate, got: $sql")
    // GROUP BY is dropped
    assert(!sql.toUpperCase.contains("GROUP BY"), s"expected no GROUP BY, got: $sql")
    // the aggregate is NOT applied to a column in the body (only declared, empty, in the head)
    assert("(?i)(max|min|sum|count|avg)\\s*\\(\\s*[\\w*]".r.findFirstIn(sql).isEmpty, s"aggregate still applied in body: $sql")

  // bag-semantic same-generation: no .distinct -> needs a CYCLE clause (PostgreSQL provides it)
  test("PostgreSQL: bag-semantic same-generation generates CYCLE"):
    import tyql.dialects.postgresql.given
    val q = Parents
      .filter(p => p.par == "A")
      .map(e => (nm = e.ch, g = 1))
      .composableFix(gensR =>
        Parents.flatMap(p =>
          gensR
            .filter(g => p.par == g.nm)
            .map(g => (nm = p.ch, g = g.g + 1))))
      .filter(g => g.g == 2)
    val sql = q.toSQLString
    println(s"[SG/Postgres] $sql")
    assert(sql.contains("UNION ALL"), sql)
    // CYCLE tracks the node-identity column (nm), not the constructed column (g)
    assert(sql.contains("CYCLE nm SET"), sql)

  // Full generated-string check for the PostgreSQL output, using the same
  // $-placeholder matcher as the other query tests (test.TestComparitor, cf.
  // expectedQueryPattern in RecursiveBenchmarkTests). `name$X` matches the actual
  // alias `name<digits>` and is checked for consistency across occurrences, so the
  // test is stable across alias renumbering. The runnable, copy-pasteable
  // PostgreSQL (single-quoted literals, no $) and its DDL are in the comment below.
  test("PostgreSQL: same-generation full SQL string (CYCLE) matches expected pattern"):
    import tyql.dialects.postgresql.given
    val q = Parents
      .filter(p => p.par == "A")
      .map(e => (nm = e.ch, g = 1))
      .composableFix(gensR =>
        Parents.flatMap(p =>
          gensR
            .filter(g => p.par == g.nm)
            .map(g => (nm = p.ch, g = g.g + 1))))
      .filter(g => g.g == 2)
    val actual = q.toSQLString.trim.replace("\n", " ").replaceAll("\\s+", " ")
    val expectedQueryPattern =
      """WITH RECURSIVE recursive$A AS ((SELECT parents$B.ch as nm, 1 as g FROM parents as parents$B WHERE parents$B.par = 'A') UNION ALL ((SELECT parents$C.ch as nm, ref$D.g + 1 as g FROM parents as parents$C, recursive$A as ref$D WHERE parents$C.par = ref$D.nm))) CYCLE nm SET is_cycle USING path SELECT * FROM recursive$A as recref$E WHERE recref$E.g = 2"""
    val (success, debug) = TestComparitor.matchStrings(expectedQueryPattern, actual)
    assert(success, s"$debug\n\nactual: $actual")

  /* ===== Runnable PostgreSQL for the test above — paste straight into psql =====
   * String literals are now rendered dialect-aware as single-quoted 'A' (see
   * Dialect.quoteStringLiteral), so the generated string runs on PostgreSQL as-is;
   * the only edits below are cosmetic alias names for readability. CYCLE is
   * PostgreSQL's guard against infinite recursion on cyclic data; here the data
   * is a DAG, so it simply terminates and the marker/path columns ride along.
   *
   * -- schema + data
   * CREATE TABLE parents(par TEXT, ch TEXT);
   * INSERT INTO parents VALUES ('A','B'), ('A','C'), ('B','D'), ('C','D');
   *
   * -- same-generation: nodes exactly 2 generations below 'A'


   *
   * -- expected rows (bag semantics: D is reached via both A->B->D and A->C->D):
   * --   nm | g | is_cycle |  path
   * --   ---+---+----------+--------------
   * --   D  | 2 | f        | {(B),(D)}-ish
   * --   D  | 2 | f        | {(C),(D)}-ish
   */

  // ---------- negative: the backend lacks the needed capability ----------

  test("PostgreSQL rejects a non-linear query (no keyed recursion)"):
    val err = compileErrors("""
      import tyql.*
      import tyql.dialects.postgresql.given
      import language.experimental.namedTuples
      import NamedTuple.*
      import scala.language.implicitConversions
      type Edge = (x: Int, y: Int)
      val Edges: Table[Edge] = Table[Edge]("edges")
      Edges.composableFix(pathR =>
        pathR.flatMap(p =>
          pathR.filter(e => p.y == e.x).map(e => (x = p.x, y = e.y))).distinct)
    """)
    assert(err.nonEmpty, "expected a compile error under PostgreSQL")
    assert(err.toLowerCase.contains("keyed recursion") || err.contains("USING KEY"), err)

  test("DuckDB rejects a bag-semantic query (no CYCLE clause)"):
    val err = compileErrors("""
      import tyql.*
      import tyql.dialects.duckdb.given
      import language.experimental.namedTuples
      import NamedTuple.*
      import scala.language.implicitConversions
      type Parent = (par: String, ch: String)
      val Parents: Table[Parent] = Table[Parent]("parents")
      Parents
        .filter(p => p.par == "A")
        .map(e => (nm = e.ch, g = 1))
        .composableFix(gensR =>
          Parents.flatMap(p =>
            gensR.filter(g => p.par == g.nm).map(g => (nm = p.ch, g = g.g + 1))))
    """)
    assert(err.nonEmpty, "expected a compile error under DuckDB")
    assert(err.contains("CYCLE"), err)

  // ---------- execution: run the generated DuckDB SQL and check RESULTS ----------

  private def withDuckDB[T](ddl: Seq[String])(body: java.sql.Connection => T): T =
    Class.forName("org.duckdb.DuckDBDriver")
    val conn = java.sql.DriverManager.getConnection("jdbc:duckdb:")
    try
      val st = conn.createStatement()
      ddl.foreach(st.execute)
      st.close()
      body(conn)
    finally conn.close()

  test("DuckDB EXECUTES the generated non-linear TC and returns the complete closure"):
    import tyql.dialects.duckdb.given
    val q = Edges.composableFix(pathR =>
      pathR.flatMap(p =>
        pathR.filter(e => p.y == e.x).map(e => (x = p.x, y = e.y))).distinct)
    val sql = q.toSQLString
    val result = withDuckDB(Seq(
      "CREATE TABLE edges(x INTEGER, y INTEGER)",
      "INSERT INTO edges VALUES (1,2),(2,3),(3,4),(2,5)"
    )) { conn =>
      val rs = conn.createStatement().executeQuery(sql)
      val buf = scala.collection.mutable.Set[(Int, Int)]()
      while rs.next() do buf += ((rs.getInt("x"), rs.getInt("y")))
      buf.toSet
    }
    // transitive closure of 1->2->{3->4, 5}
    val expected = Set((1,2),(1,3),(1,4),(1,5),(2,3),(2,4),(2,5),(3,4))
    assertEquals(result, expected, s"generated SQL:\n$sql")

  test("DuckDB EXECUTES the generated recursive aggregation (BOM) with correct max-days"):
    import tyql.dialects.duckdb.given
    import Expr.max
    val q = BasicParts.composableFix(waitFor =>
      SubParts.aggregate(sp =>
        waitFor
          .filter(wf => sp.sub == wf.part)
          .aggregate(wf => (part = sp.part, days = max(wf.days))))
        .groupBySource((wf, _) => (part = wf.part))
        .distinct)
    val sql = q.toSQLString
    val result = withDuckDB(Seq(
      "CREATE TABLE basicparts(part VARCHAR, days INTEGER)",
      "INSERT INTO basicparts VALUES ('A',1),('B',2)",
      "CREATE TABLE subparts(part VARCHAR, sub VARCHAR)",
      "INSERT INTO subparts VALUES ('C','A'),('C','B'),('D','C')"
    )) { conn =>
      val rs = conn.createStatement().executeQuery(sql)
      val buf = scala.collection.mutable.Map[String, Int]()
      while rs.next() do
        val p = rs.getString("part"); val d = rs.getInt("days")
        buf(p) = math.max(buf.getOrElse(p, Int.MinValue), d)
      buf.toMap
    }
    assertEquals(result, Map("A" -> 1, "B" -> 2, "C" -> 2, "D" -> 2), s"generated SQL:\n$sql")

  test("DuckDB EXECUTES a generated string-literal filter (single-quoted) correctly"):
    import tyql.dialects.duckdb.given
    // end-to-end check of Dialect.quoteStringLiteral: the generated `WHERE par = 'A'`
    // must be a string literal (single quotes). Rendered with double quotes it would
    // be read as an identifier and DuckDB would error "column A does not exist".
    val q = Parents.filter(p => p.par == "A").map(p => (ch = p.ch))
    val sql = q.toSQLString
    assert(sql.contains("= 'A'"), sql)
    val result = withDuckDB(Seq(
      "CREATE TABLE parents(par VARCHAR, ch VARCHAR)",
      "INSERT INTO parents VALUES ('A','B'),('A','C'),('B','D')"
    )) { conn =>
      val rs = conn.createStatement().executeQuery(sql)
      val buf = scala.collection.mutable.Set[String]()
      while rs.next() do buf += rs.getString("ch")
      buf.toSet
    }
    assertEquals(result, Set("B", "C"), s"generated SQL:\n$sql")

  test("PostgreSQL rejects aggregation in the recursive step"):
    val err = compileErrors("""
      import tyql.*
      import tyql.dialects.postgresql.given
      import tyql.Expr.max
      import language.experimental.namedTuples
      import NamedTuple.*
      import scala.language.implicitConversions
      type BasicPart = (part: String, days: Int)
      type SubPart = (part: String, sub: String)
      val BasicParts: Table[BasicPart] = Table[BasicPart]("basicparts")
      val SubParts: Table[SubPart] = Table[SubPart]("subparts")
      BasicParts.composableFix(waitFor =>
        SubParts.aggregate(sp =>
          waitFor.filter(wf => sp.sub == wf.part)
            .aggregate(wf => (part = sp.part, days = max(wf.days))))
          .groupBySource((wf, _) => (part = wf.part))
          .distinct)
    """)
    assert(err.nonEmpty, "expected a compile error under PostgreSQL")
