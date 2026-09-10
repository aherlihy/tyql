package test.query.linearunion

import language.experimental.namedTuples
import tyql.Query.fix

/**
 * Tests for linearity across `union`/`unionAll`.
 *
 * A `union` becomes two separate Datalog rules, so a recursive reference used once in each
 * branch must count once, not twice: it must NOT be reported as a linearity violation.
 * A duplicate WITHIN a single branch (a self-join of the recursive relation) must still be
 * rejected. These are enforced by `UnionCollect` (multiset union / pointwise-max) on the
 * dependency tuple, in place of `Tuple.Concat` (which is kept for `flatMap`, a join).
 *
 * Positive tests assert the query compiles (empty error string); negative tests assert the
 * expected linearity error is still produced. Both affine (no reference used twice per rule)
 * and relevance (every recursive relation used at least once) are covered.
 */

// ---------------------------------------------------------------------------
// AFFINE
// ---------------------------------------------------------------------------

/** POSITIVE: the recursive relation is used once in each branch of a union -> linear. */
class LinearUnionAffinePositiveTest extends munit.FunSuite {
  test("union of two linear recursive branches is accepted") {
    val error: String =
      compileErrors(
        """
          import language.experimental.namedTuples
          import tyql.{Table, Expr, Query}
          import tyql.Query.fix

          type Edge = (x: Int, y: Int)
          val tables = (
            edges  = Table[Edge]("edges"),
            edges2 = Table[Edge]("edges2"),
            emptyEdges = Table[Edge]("empty")
          )

          val path = tables.edges
          path.fix(path =>
            val left  = path.flatMap(p =>
              tables.edges.filter(e => p.y == e.x).map(e => (x = p.x, y = e.y).toRow)
            )
            val right = path.flatMap(p =>
              tables.edges2.filter(e => p.y == e.x).map(e => (x = p.x, y = e.y).toRow)
            )
            val joined = left.union(right)
            joined
          )
        """)
    assert(error.isEmpty, s"Expected the linear union to compile, but got:\n$error")
  }
}

/** POSITIVE: same as above with `unionAll` (bag) plus `distinct` to satisfy the set category. */
class LinearUnionAllAffinePositiveTest extends munit.FunSuite {
  test("unionAll of two linear recursive branches is accepted") {
    val error: String =
      compileErrors(
        """
          import language.experimental.namedTuples
          import tyql.{Table, Expr, Query}
          import tyql.Query.fix

          type Edge = (x: Int, y: Int)
          val tables = (
            edges  = Table[Edge]("edges"),
            edges2 = Table[Edge]("edges2"),
            emptyEdges = Table[Edge]("empty")
          )

          val path = tables.edges
          path.fix(path =>
            path.flatMap(p =>
              tables.edges.filter(e => p.y == e.x).map(e => (x = p.x, y = e.y).toRow)
            ).unionAll(
              path.flatMap(p =>
                tables.edges2.filter(e => p.y == e.x).map(e => (x = p.x, y = e.y).toRow)
              )
            ).distinct
          )
        """)
    assert(error.isEmpty, s"Expected the linear unionAll to compile, but got:\n$error")
  }
}

/** NEGATIVE: one union branch self-joins the recursive relation -> non-linear branch survives. */
class LinearUnionAffineNegativeTest extends munit.FunSuite {
  // The union result carries the duplicated dependency ((0 : Int), (0 : Int)), which the fix
  // constraint rejects (it requires Tuple1[(0 : Int)]).
  def expectedError: String = "((0 : Int), (0 : Int))"

  test("union with a self-joined (non-linear) branch is rejected") {
    val error: String =
      compileErrors(
        """
          import language.experimental.namedTuples
          import tyql.{Table, Expr, Query}
          import tyql.Query.fix

          type Edge = (x: Int, y: Int)
          val tables = (
            edges  = Table[Edge]("edges"),
            edges2 = Table[Edge]("edges2"),
            emptyEdges = Table[Edge]("empty")
          )

          val path = tables.edges
          path.fix(path =>
            path.flatMap(p =>
              path.filter(e => p.y == e.x).map(e => (x = p.x, y = e.y).toRow)
            ).union(
              path.flatMap(p =>
                tables.edges2.filter(e => p.y == e.x).map(e => (x = p.x, y = e.y).toRow)
              )
            )
          )
        """)
    assert(error.contains(expectedError), s"Expected substring '$expectedError' in:\n$error")
  }
}

// ---------------------------------------------------------------------------
// RELEVANCE (every recursive relation must be used at least once)
// ---------------------------------------------------------------------------

/** POSITIVE: a union spans both recursive relations, so `UnionCollect` must keep both tags. */
class LinearUnionRelevancePositiveTest extends munit.FunSuite {
  test("union covering both mutually-recursive relations satisfies relevance") {
    val error: String =
      compileErrors(
        """
          import language.experimental.namedTuples
          import tyql.{Table, Expr, Query}
          import tyql.Query.fix

          type Edge = (x: Int, y: Int)
          val tables = (
            edges  = Table[Edge]("edges"),
            edges2 = Table[Edge]("edges2"),
            emptyEdges = Table[Edge]("empty")
          )

          val pathBase  = tables.edges
          val path2Base = tables.emptyEdges
          val (r1, r2) = fix(pathBase, path2Base)((path, path2) =>
            // definition of relation 1 uses BOTH path and path2, once each, via a union
            val P = path.flatMap(p =>
                      tables.edges.filter(e => p.y == e.x).map(e => (x = p.x, y = e.y).toRow)
                    ).union(
                      path2.flatMap(p =>
                        tables.edges.filter(e => p.y == e.x).map(e => (x = p.x, y = e.y).toRow)
                      )
                    )
            val P2 = path2.flatMap(p =>
                       tables.edges.filter(e => p.y == e.x).map(e => (x = p.x, y = e.y).toRow)
                     ).distinct
            (P, P2)
          )
          r2
        """)
    assert(error.isEmpty, s"Expected the relation-covering union to compile, but got:\n$error")
  }
}

/** NEGATIVE: a mutually-recursive relation is never used -> relevance violation still caught. */
class LinearUnionRelevanceNegativeTest extends munit.FunSuite {
  def expectedError: String =
    "Recursive definitions must be linear, e.g. recursive references must appear at least once in all the recursive definitions"

  test("union that never uses one recursive relation is rejected (relevance)") {
    val error: String =
      compileErrors(
        """
          import language.experimental.namedTuples
          import tyql.{Table, Expr, Query}
          import tyql.Query.fix

          type Edge = (x: Int, y: Int)
          val tables = (
            edges  = Table[Edge]("edges"),
            edges2 = Table[Edge]("edges2"),
            emptyEdges = Table[Edge]("empty")
          )

          val pathBase  = tables.edges
          val path2Base = tables.emptyEdges
          val (r1, r2) = fix(pathBase, path2Base)((path, path2) =>
            // both definitions only ever reference `path`; `path2` is never used
            val P = path.flatMap(p =>
                      tables.edges.filter(e => p.y == e.x).map(e => (x = p.x, y = e.y).toRow)
                    ).union(
                      path.flatMap(p =>
                        tables.edges2.filter(e => p.y == e.x).map(e => (x = p.x, y = e.y).toRow)
                      )
                    )
            val P2 = path.flatMap(p =>
                       tables.edges.filter(e => p.y == e.x).map(e => (x = p.x, y = e.y).toRow)
                     ).distinct
            (P, P2)
          )
          r2
        """)
    assert(error.contains(expectedError), s"Expected substring '$expectedError' in:\n$error")
  }
}
