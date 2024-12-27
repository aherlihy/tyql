//> using jvm "21"
//> using options "-experimental"
//> using dep "ch.epfl.lamp::tyql:0.0.1"
//> using dep "com.lihaoyi::os-lib-watch:0.11.3"
//> using dep "com.lihaoyi::pprint:0.9.0"
//> using dep "com.lihaoyi::ujson:4.0.2"

import pprint.pprintln


/*
  TODO:
  - have the snippets return the results of running the SQL against the DB
  - generate markdown
*/

/// Remember to publishLocal the library with the inline flag `shouldTrackPolyfillUsage` set to true!
/// sudo mkdir /mnt/ramdisk
/// sudo mount -t tmpfs -o size=1g tmpfs /mnt/ramdisk
val directory = "/mnt/ramdisk/"

object dialects {
  val postgresql = "postgresql"
  val mysql      = "mysql"
  val mariadb    = "mariadb"
  val sqlite     = "sqlite"
  val duckdb     = "duckdb"
  val h2         = "h2"
}

object dialect_features {
  val randomUUID        = "randomUUID"
  val randomInt         = "randomInt"
  val reversibleStrings = "reversibleStrings"
}

val dialect_imports = Map(
  dialects.postgresql -> "import tyql.Dialect.postgresql.given",
  dialects.mysql      -> "import tyql.Dialect.mysql.given",
  dialects.mariadb    -> "import tyql.Dialect.mariadb.given",
  dialects.sqlite     -> "import tyql.Dialect.sqlite.given",
  dialects.duckdb     -> "import tyql.Dialect.duckdb.given",
  dialects.h2         -> "import tyql.Dialect.h2.given"
)

val dialect_features_uses = Map(
  dialect_features.randomUUID        -> "tyql.Expr.randomUUID()",
  dialect_features.randomInt         -> "tyql.Expr.randomInt(0, tyql.lit(1) + tyql.lit(1))",
  dialect_features.reversibleStrings -> "tyql.Expr.reverse(tyql.lit(\"abc\"))"
)

// WARNING XXX: this needs `sbt publishLocal` first
def testScript(dialectSnippet: String, codeSnippet: String) = "" +
s"""//> using jvm "21"
//> using options "-experimental"
//> using dep "ch.epfl.lamp::tyql:0.0.1"
//> using dep "com.lihaoyi::ujson:4.0.2"

import tyql.*

$dialectSnippet

case class R(i: Int)
val t = tyql.Table[R]("t")
var sql = ""
var exception = false

@main def main() = {
  try {
    sql = t.map(_ => ($codeSnippet)).toQueryIR.toSQLString()
  } catch {
    case _ => exception = true
  } finally {
    val js = if exception then
      ujson.Obj("exception" -> true)
    else
      ujson.Obj("exception" -> false, "sql" -> sql, "wasPolyfillUsed" -> tyql.wasPolyfillUsed)
    println(ujson.write(js))
  }
}

"""

@main def main() = {
  val matrix = dialect_features_uses.map { case (feature, codeSnippet) =>
    println("FEATURE " + feature)
    val dialectResults = dialect_imports.map { case (dialect, dialectSnippet) =>
      println("DIALECT " + dialect)
      val script = testScript(dialectSnippet, codeSnippet)
      val path = s"${directory}main.scala"
      os.write.over(os.Path(path), script)
      val compilationAndRunStatus =
        os.proc("scala-cli", path)
          .call(cwd=os.Path(directory), check=false)
      if (compilationAndRunStatus.exitCode != 0) {
        dialect -> (compilationAndRunStatus.exitCode == 0)
      } else {
        val text = compilationAndRunStatus.out.text().trim()
        val result = ujson.read(text.split("\n").last)
        assert(result("exception").bool == false)
        dialect -> (true, result("wasPolyfillUsed").bool, result("sql").str)
      }
    }.toMap

    feature -> dialectResults
  }

  pprintln(matrix)
}

/*
Map(
  "randomUUID" -> HashMap(
    "sqlite" -> false,
    "postgresql" -> (true, false, "SELECT gen_random_uuid() FROM t as t0"),
    "h2" -> (true, false, "SELECT RANDOM_UUID() FROM t as t0"),
    "mariadb" -> (true, false, "SELECT UUID() FROM t as t0"),
    "mysql" -> (true, false, "SELECT UUID() FROM t as t0"),
    "duckdb" -> (true, false, "SELECT uuid() FROM t as t0")
  ),
  "randomInt" -> HashMap(
    "sqlite" -> (
      true,
      true,
      "SELECT (with randomIntParameters as (select 0 as a, 1 + 1 as b) select cast(abs(random() % (b - a + 1) + a) as integer) from randomIntParameters) FROM t as t0"
    ),
    "postgresql" -> (
      true,
      true,
      "SELECT (with randomIntParameters as (select 0 as a, 1 + 1 as b) select floor(random() * (b - a + 1) + a)::integer from randomIntParameters) FROM t as t0"
    ),
    "h2" -> (
      true,
      true,
      "SELECT (with randomIntParameters as (select 0 as a, 1 + 1 as b) select floor(rand() * (b - a + 1) + a) from randomIntParameters) FROM t as t0"
    ),
    "mariadb" -> (
      true,
      true,
      "SELECT (with randomIntParameters as (select 0 as a, 1 + 1 as b) select floor(rand() * (b - a + 1) + a) from randomIntParameters) FROM t as t0"
    ),
    "mysql" -> (
      true,
      true,
      "SELECT (with randomIntParameters as (select 0 as a, 1 + 1 as b) select floor(rand() * (b - a + 1) + a) from randomIntParameters) FROM t as t0"
    ),
    "duckdb" -> (
      true,
      true,
      "SELECT (with randomIntParameters as (select 0 as a, 1 + 1 as b) select floor(random() * (b - a + 1) + a)::integer from randomIntParameters) FROM t as t0"
    )
  ),
  "reversibleStrings" -> HashMap(
    "sqlite" -> (true, false, "SELECT REVERSE('abc') FROM t as t0"),
    "postgresql" -> (true, false, "SELECT REVERSE('abc') FROM t as t0"),
    "h2" -> false,
    "mariadb" -> (true, false, "SELECT REVERSE('abc') FROM t as t0"),
    "mysql" -> (true, false, "SELECT REVERSE('abc') FROM t as t0"),
    "duckdb" -> (true, false, "SELECT REVERSE('abc') FROM t as t0")
  )
)
*/
