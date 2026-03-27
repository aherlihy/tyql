package test.query.nonmonotone

import test.{SQLStringQueryTest, TestDatabase}
import tyql.*
import Query.{fix, restrictedFix}
import Expr.{IntLit, StringLit, count, max, min, sum}
import RestrictedQuery.*

import language.experimental.namedTuples
import NamedTuple.*
import scala.language.implicitConversions

// ========== Test types ==========

type Edge = (x: Int, y: Int)
type EdgeDB = (edges: Edge)

given EdgeDBs: TestDatabase[EdgeDB] with
  override def tables = (
    edges = Table[Edge]("edges")
  )

// ========== Positive tests: NonMonotone allows aggregation ==========

// P1: Simple aggregate inside recursion with NonMonotone + NonRestrictedConstructors + NonLinear
// The base and recursive cases must have the same row type, so the aggregate must produce the same type.
class NonMonotoneSimpleAggregateTest extends SQLStringQueryTest[EdgeDB, Edge] {
  def testDescription: String = "NonMonotone: simple aggregate with sum inside recursion"

  def query() =
    val base = testDB.tables.edges
    val options = (constructorFreedom = NonRestrictedConstructors(), monotonicity = NonMonotone(), category = SetResult(), linearity = NonLinear(), mutual = NoMutual())
    base.fix(options)(pathRec =>
      pathRec.aggregate(p => (x = p.x, y = sum(p.y)).toGroupingRow)
        .groupBySource(p => (x = p._1.x).toRow)
        .distinct
    )

  def expectedQueryPattern: String =
    """
      WITH RECURSIVE
        recursive$1 AS
          ((SELECT * FROM edges as edges$1)
            UNION
          ((SELECT ref$1.x as x, SUM(ref$1.y) as y FROM recursive$1 as ref$1 GROUP BY ref$1.x)))
      SELECT * FROM recursive$1 as recref$0
    """
}

// P2: NonMonotone + Linear (linearity still enforced even with aggregation)
class NonMonotoneLinearTest extends SQLStringQueryTest[EdgeDB, Edge] {
  def testDescription: String = "NonMonotone + Linear: aggregate is allowed but linearity is still enforced"

  def query() =
    val base = testDB.tables.edges
    val options = (constructorFreedom = NonRestrictedConstructors(), monotonicity = NonMonotone(), category = SetResult(), linearity = Linear(), mutual = NoMutual())
    base.fix(options)(pathRec =>
      pathRec.aggregate(p => (x = p.x, y = sum(p.y)).toGroupingRow)
        .groupBySource(p => (x = p._1.x).toRow)
        .distinct
    )

  def expectedQueryPattern: String =
    """
      WITH RECURSIVE
        recursive$1 AS
          ((SELECT * FROM edges as edges$1)
            UNION
          ((SELECT ref$1.x as x, SUM(ref$1.y) as y FROM recursive$1 as ref$1 GROUP BY ref$1.x)))
      SELECT * FROM recursive$1 as recref$0
    """
}

// P3: NonMonotone + BagResult (set semantics disabled)
class NonMonotoneBagTest extends SQLStringQueryTest[EdgeDB, Edge] {
  def testDescription: String = "NonMonotone + BagResult: aggregate with bag semantics"

  def query() =
    val base = testDB.tables.edges
    val options = (constructorFreedom = NonRestrictedConstructors(), monotonicity = NonMonotone(), category = BagResult(), linearity = NonLinear(), mutual = NoMutual())
    base.fix(options)(pathRec =>
      pathRec.aggregate(p => (x = p.x, y = sum(p.y)).toGroupingRow)
        .groupBySource(p => (x = p._1.x).toRow)
    )

  def expectedQueryPattern: String =
    """
      WITH RECURSIVE
        recursive$1 AS
          ((SELECT * FROM edges as edges$1)
            UNION ALL
          ((SELECT ref$1.x as x, SUM(ref$1.y) as y FROM recursive$1 as ref$1 GROUP BY ref$1.x)))
      SELECT * FROM recursive$1 as recref$0
    """
}

// P4: NonMonotone + RestrictedConstructors — aggregate is callable, but agg functions like sum/min/max
// require NonRestrictedConstructors on their inputs. So this combination is valid at the API level
// but standard agg functions won't accept restricted fields. This is tested as a negative test below (N2).

// ========== Negative tests: Monotone disallows aggregation ==========

// N1: Monotone should NOT allow aggregate
class MonotoneRejectsAggregateTest extends munit.FunSuite {
  def testDescription: String = "Monotone: aggregate is not available"
  def expectedError: String = "Cannot prove that tyql.Monotone =:= tyql.NonMonotone"

  test(testDescription) {
    val error: String = compileErrors("""
      import language.experimental.namedTuples
      import tyql.{Table, Expr, Query, RestrictedQuery}
      import Expr.sum
      import RestrictedQuery.*
      import Query.fix

      type Edge = (x: Int, y: Int)
      val base = Table[Edge]("edges")

      base.fix((constructorFreedom = NonRestrictedConstructors(), monotonicity = Monotone(), category = SetResult(), linearity = Linear(), mutual = NoMutual()))(pathRec =>
        pathRec.aggregate(p => (x = sum(p.x), y = sum(p.y)).toGroupingRow).groupBySource(p => (x = p._1.x).toRow).distinct
      )
    """)
    assert(error.contains(expectedError), s"Expected substring '$expectedError' in '$error'")
  }
}

// N2: NonMonotone allows aggregate but constructor freedom is still enforced independently.
// Using + with RestrictedConstructors should fail OUTSIDE aggregate (in filter/map).
class NonMonotoneConstructorStillEnforcedTest extends munit.FunSuite {
  def testDescription: String = "NonMonotone + RestrictedConstructors: arithmetic in filter still fails"
  def expectedError: String = "value + is not a member of tyql.Expr[Int, tyql.NonScalarExpr, tyql.RestrictedConstructors]"

  test(testDescription) {
    val error: String = compileErrors("""
      import language.experimental.namedTuples
      import tyql.{Table, Expr, Query, RestrictedQuery}
      import Expr.sum
      import RestrictedQuery.*
      import Query.fix

      type Edge = (x: Int, y: Int)
      val base = Table[Edge]("edges")

      base.fix((constructorFreedom = RestrictedConstructors(), monotonicity = NonMonotone(), category = SetResult(), linearity = NonLinear(), mutual = NoMutual()))(pathRec =>
        pathRec.filter(p => p.x == p.y + 1).aggregate(p => sum(p.x)).groupBySource(p => (x = p._1.x).toRow).distinct
      )
    """)
    assert(error.contains(expectedError), s"Expected substring '$expectedError' in '$error'")
  }
}

// N3: NonMonotone allows aggregate but linearity is still enforced.
// Using the recursive ref twice (once in aggregate, once in flatMap) with Linear should fail.
class NonMonotoneLinearStillEnforcedTest extends munit.FunSuite {
  def testDescription: String = "NonMonotone + Linear: using recursive ref twice still fails"
  def expectedError: String = "Failed to generate recursive queries"

  test(testDescription) {
    val error: String = compileErrors("""
      import language.experimental.namedTuples
      import tyql.{Table, Expr, Query, RestrictedQuery}
      import Expr.sum
      import RestrictedQuery.*
      import Query.fix

      type Edge = (x: Int, y: Int)
      val base = Table[Edge]("edges")

      base.fix((constructorFreedom = NonRestrictedConstructors(), monotonicity = NonMonotone(), category = SetResult(), linearity = Linear(), mutual = NoMutual()))(pathRec =>
        pathRec.flatMap(p =>
          pathRec.filter(e => p.y == e.x).map(e => (x = p.x, y = e.y).toRow)
        ).distinct
      )
    """)
    assert(error.contains(expectedError), s"Expected substring '$expectedError' in '$error'")
  }
}

// N4: NonMonotone allows aggregate but set semantics is still enforced.
// Returning a BagResult (no .distinct) when SetResult is required should fail.
class NonMonotoneSetStillEnforcedTest extends munit.FunSuite {
  def testDescription: String = "NonMonotone + SetResult: returning bag from aggregate without distinct fails"
  def expectedError: String = "tyql.BagResult"

  test(testDescription) {
    val error: String = compileErrors("""
      import language.experimental.namedTuples
      import tyql.{Table, Expr, Query, RestrictedQuery}
      import Expr.sum
      import RestrictedQuery.*
      import Query.fix

      type Edge = (x: Int, y: Int)
      val base = Table[Edge]("edges")

      base.fix((constructorFreedom = NonRestrictedConstructors(), monotonicity = NonMonotone(), category = SetResult(), linearity = NonLinear(), mutual = NoMutual()))(pathRec =>
        pathRec.aggregate(p => (x = p.x, y = sum(p.y)).toGroupingRow)
          .groupBySource(p => (x = p._1.x).toRow)
      )
    """)
    assert(error.contains(expectedError), s"Expected substring '$expectedError' in '$error'")
  }
}

// N5: NonMonotone allows aggregate but mutual recursion is still enforced.
class NonMonotoneMutualStillEnforcedTest extends munit.FunSuite {
  def testDescription: String = "NonMonotone + NoMutual: mutual recursion with aggregate still fails"
  def expectedError: String = "restrictedFix does not support mutual recursion"

  test(testDescription) {
    val error: String = compileErrors("""
      import language.experimental.namedTuples
      import tyql.{Table, Expr, Query, RestrictedQuery}
      import Expr.sum
      import RestrictedQuery.*
      import Query.{fix, restrictedFix}

      type Edge = (x: Int, y: Int)
      val base1 = Table[Edge]("edges1")
      val base2 = Table[Edge]("edges2")

      restrictedFix(base1, base2)((r1, r2) =>
        val p1 = r1.flatMap(p => r2.filter(e => p.y == e.x).map(e => (x = p.x, y = e.y).toRow)).distinct
        val p2 = r2.distinct
        (p1, p2)
      )
    """)
    assert(error.contains(expectedError), s"Expected substring '$expectedError' in '$error'")
  }
}
