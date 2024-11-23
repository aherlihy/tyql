//> using jvm "21"
//> using options "-experimental"
//> using dep "ch.epfl.lamp::tyql:0.0.1"
//> using dep "com.lihaoyi::os-lib-watch:0.11.3"
//> using dep "com.lihaoyi::pprint:0.9.0"

import pprint.pprintln


/*
  For now we are not yet writing the exact documentation/referece/tutorial,
  but we are building the infrastructure for generating it.

  TODO:
  - have the snippets return some values that can be read back
  - have the snippets return SQL
  - have the snippets return the results of running the SQL against the DB
  - tracking of polyfills
  - generate markdown
  - generate HTML
*/

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

import tyql.*

$dialectSnippet

@main def main() = {
  try {

    $codeSnippet

  } catch {
    case _ => println("exception")
  }
}
"""

@main def main() = {
  val NullOutput = os.ProcessOutput((_, _) => ())
  val matrix = dialect_features_uses.map { case (feature, codeSnippet) =>
    val dialectResults = dialect_imports.map { case (dialect, dialectSnippet) =>
      val script = testScript(dialectSnippet, codeSnippet)
      val path = s"${directory}main.scala"
      os.write.over(os.Path(path), script)
      val compilationAndRunStatus =
        os.proc("scala-cli", path)
          .call(cwd=os.Path(directory), check=false, stdout=NullOutput, stderr=NullOutput)
      dialect -> (compilationAndRunStatus.exitCode == 0)
    }.toMap

    feature -> dialectResults
  }

  pprintln(matrix)
}

/*
Map(
  "randomUUID" -> HashMap(
    "sqlite" -> false,
    "postgresql" -> true,
    "h2" -> true,
    "mariadb" -> true,
    "mysql" -> true,
    "duckdb" -> true
  ),
  "randomInt" -> HashMap(
    "sqlite" -> true,
    "postgresql" -> true,
    "h2" -> true,
    "mariadb" -> true,
    "mysql" -> true,
    "duckdb" -> true
  ),
  "reversibleStrings" -> HashMap(
    "sqlite" -> true,
    "postgresql" -> true,
    "h2" -> false,
    "mariadb" -> true,
    "mysql" -> true,
    "duckdb" -> true
  )
)
*/
