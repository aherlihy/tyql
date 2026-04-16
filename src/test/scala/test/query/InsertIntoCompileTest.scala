package test.query.insertinto

/** Inserting a row whose `Long` column doesn't match a target table's
 *  `String` column, compared across TyQL and ScalaSQL.  Both tests exercise
 *  the *same* operation (INSERT INTO … SELECT) on each library, so the
 *  difference in error shape reflects a real difference in the libraries'
 *  error-reporting rather than a difference in the operation being compiled. */
class InsertIntoCompileTest extends munit.FunSuite:

  // ---------------- TyQL ----------------

  /** Exact message emitted by the `insertInto` row-type check.  The
   *  `compileErrors` output prefixes this with `error: ` and follows it with
   *  the offending source line + caret, so we verify containment of this
   *  sentence and also that the compile error is singular. */
  private val tyqlExpectedMessage: String =
    "Types being inserted (name : Long) do not fit inside target types (name : String)."

  test("TyQL: inserting Long into String emits the expected focused error"):
    val error: String =
      compileErrors(
        """
          import tyql.*
          import tyql.dialects.postgresql.given
          import scala.language.implicitConversions

          type Row = (name: String)
          val t: Table[Row] = Table[Row]("t")
          val src: Query[(name: Long), BagResult] = ???
          src.insertInto(t)
        """)

    assert(error.nonEmpty, "expected a compile error but the snippet compiled")
    assert(error.contains(tyqlExpectedMessage),
      s"expected the error to contain:\n  $tyqlExpectedMessage\ngot:\n$error")

    // Exactly one error — no cascading noise.
    assertEquals(error.split("error:").length - 1, 1,
      s"expected exactly one compile error, got:\n$error")

    // Focused: no reference to any library-internal machinery.
    for internal <- Seq("Queryable", "ExprQueryable", "Select[C", "type variable") do
      assert(!error.contains(internal),
        s"did not expect '$internal' in a focused TyQL error:\n$error")

  // ---------------- ScalaSQL ----------------

  test("ScalaSQL: same-shape INSERT INTO ... SELECT mismatch produces a verbose error"):
    val error: String =
      compileErrors(
        """
          import scalasql.*
          import scalasql.PostgresDialect.*

          case class Src[T[_]](v: T[Long])
          case class Dst[T[_]](v: T[String])
          object Src extends Table[Src]()
          object Dst extends Table[Dst]()

          // INSERT INTO Dst SELECT * FROM Src, where Src.v : Long and Dst.v : String.
          Dst.insert.select(x => x, Src.select)
        """)

    assert(error.nonEmpty, "expected a compile error but the snippet compiled")

    // Stable phrases that together reconstruct the Scala compiler's
    // Found/Required implicit-mismatch on scalasql.query.Select[C, R2].
    val expectedPhrases = Seq(
      "Found:",
      "Required:",
      "scalasql.query.Select",
      "type variable with constraint",
      "Src[scalasql.core.Expr]",
      "Dst[scalasql.core.Expr]",
    )
    for phrase <- expectedPhrases do
      assert(error.contains(phrase),
        s"expected ScalaSQL error to mention '$phrase'. Full error:\n$error")

    // Verbose: the paper's point is that the error spans multiple lines and
    // names internal library types + type variables.
    val lineCount = error.split('\n').length
    assert(lineCount >= 4,
      s"expected a multi-line error (>= 4 lines), got $lineCount:\n$error")
