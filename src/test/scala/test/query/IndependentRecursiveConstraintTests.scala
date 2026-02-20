package test.query.independentconstraints

import language.experimental.namedTuples

/**
 * Tests that each recursive constraint (monotonicity, mutual recursion, linearity, set semantics,
 * constructor freedom) is independently enforceable. Each test enables ONE constraint while
 * disabling all others, and verifies that violations of that single constraint are caught.
 */

// T1: Constructor freedom is enforced independently.
// All other constraints disabled (NonMonotone, AllowMutual, NonLinear, BagResult).
// Using + (arithmetic constructor) with RestrictedConstructors should fail.
class ConstructorFreedomAloneTest extends munit.FunSuite {
  def expectedError: String = "value + is not a member of tyql.Expr[Int, tyql.NonScalarExpr, tyql.RestrictedConstructors]"

  test("Constructor freedom enforced when all other constraints disabled") {
    val error = compileErrors("""
      import language.experimental.namedTuples
      import tyql.*
      import Query.fix

      type Edge = (x: Int, y: Int)
      val base = Table[Edge]("edges")

      base.fix((constructorFreedom = RestrictedConstructors(), monotonicity = NonMonotone(), category = BagResult(), linearity = NonLinear(), mutual = NoMutual()))(pathRec =>
        pathRec.flatMap(p =>
          pathRec.filter(e => p.y == e.x + 1).map(e => (x = p.x, y = e.y).toRow)
        )
      )
    """)
    assert(error.contains(expectedError), s"Expected substring '$expectedError' in '$error'")
  }
}

// T2: Set semantics enforced independently.
// All other constraints disabled (NonRestrictedConstructors, NonMonotone, AllowMutual, NonLinear).
// Returning BagResult when SetResult required should fail.
class SetSemanticsAloneTest extends munit.FunSuite {
  def expectedError: String = "tyql.BagResult"

  test("Set semantics enforced when all other constraints disabled") {
    val error = compileErrors("""
      import language.experimental.namedTuples
      import tyql.*
      import Query.fix

      type Edge = (x: Int, y: Int)
      val base = Table[Edge]("edges")

      base.fix((constructorFreedom = NonRestrictedConstructors(), monotonicity = NonMonotone(), category = SetResult(), linearity = NonLinear(), mutual = NoMutual()))(pathRec =>
        pathRec.flatMap(p =>
          pathRec.filter(e => p.y == e.x).map(e => (x = p.x, y = e.y).toRow)
        )
      )
    """)
    assert(error.contains(expectedError), s"Expected substring '$expectedError' in '$error'")
  }
}

// T3: Linearity (relevance) enforced independently.
// All other constraints disabled (NonRestrictedConstructors, NonMonotone, AllowMutual, BagResult).
// Mutual recursion with a ref that is never used should fail relevance.
class LinearRelevanceAloneTest extends munit.FunSuite {
  def expectedError: String = "Failed to generate recursive queries"

  test("Linearity (relevance) enforced when all other constraints disabled") {
    val error = compileErrors("""
      import language.experimental.namedTuples
      import tyql.*
      import Query.fix

      type Edge = (x: Int, y: Int)
      val base1 = Table[Edge]("edges1")
      val base2 = Table[Edge]("edges2")

      fix((constructorFreedom = NonRestrictedConstructors(), monotonicity = NonMonotone(), category = BagResult(), linearity = Linear(), mutual = AllowMutual()))(base1, base2)((r1, r2) =>
        val p1 = r1.flatMap(p => r1.filter(e => p.y == e.x).map(e => (x = p.x, y = e.y).toRow))
        val p2 = r1.map(e => (x = e.x, y = e.y).toRow)
        (p1, p2)
      )
    """)
    assert(error.contains(expectedError), s"Expected substring '$expectedError' in '$error'")
  }
}

// T5: Mutual recursion enforced independently via fix.
// fix always disallows mutual recursion regardless of other settings.
class MutualRecursionViaFixTest extends munit.FunSuite {
  def expectedError: String = "restrictedFix does not support mutual recursion"

  test("Mutual recursion rejected by fix") {
    val error = compileErrors("""
      import language.experimental.namedTuples
      import tyql.*
      import Query.restrictedFix

      type Edge = (x: Int, y: Int)
      val base1 = Table[Edge]("edges1")
      val base2 = Table[Edge]("edges2")

      restrictedFix(base1, base2)((r1, r2) =>
        val p1 = r1.flatMap(p => r2.filter(e => p.y == e.x).map(e => (x = p.x, y = e.y).toRow)).distinct
        val p2 = r2.flatMap(p => r1.filter(e => p.y == e.x).map(e => (x = p.x, y = e.y).toRow)).distinct
        (p1, p2)
      )
    """)
    assert(error.contains(expectedError), s"Expected substring '$expectedError' in '$error'")
  }
}

// T6: Mutual recursion enforced independently via customFix with NoMutual.
class MutualRecursionViaCustomFixTest extends munit.FunSuite {
  def expectedError: String = "Mutual recursion is not allowed"

  test("Mutual recursion rejected by fix with NoMutual") {
    val error = compileErrors("""
      import language.experimental.namedTuples
      import tyql.*
      import Query.fix

      type Edge = (x: Int, y: Int)
      val base1 = Table[Edge]("edges1")
      val base2 = Table[Edge]("edges2")

      fix((constructorFreedom = NonRestrictedConstructors(), monotonicity = NonMonotone(), category = BagResult(), linearity = NonLinear(), mutual = NoMutual()))(base1, base2)((r1, r2) =>
        val p1 = r1.flatMap(p => r2.filter(e => p.y == e.x).map(e => (x = p.x, y = e.y).toRow))
        val p2 = r2.map(e => (x = e.x, y = e.y).toRow)
        (p1, p2)
      )
    """)
    assert(error.contains(expectedError), s"Expected substring '$expectedError' in '$error'")
  }
}

// T7: Monotonicity enforced independently.
// All other constraints disabled. Calling aggregate with Monotone should fail.
class MonotonicityAloneTest extends munit.FunSuite {
  def expectedError: String = "Cannot prove that tyql.Monotone =:= tyql.NonMonotone"

  test("Monotonicity enforced when all other constraints disabled") {
    val error = compileErrors("""
      import language.experimental.namedTuples
      import tyql.*
      import Expr.sum
      import RestrictedQuery.*
      import Query.fix

      type Edge = (x: Int, y: Int)
      val base = Table[Edge]("edges")

      base.fix((constructorFreedom = NonRestrictedConstructors(), monotonicity = Monotone(), category = BagResult(), linearity = NonLinear(), mutual = NoMutual()))(pathRec =>
        pathRec.aggregate(p => (x = p.x, y = sum(p.y)).toGroupingRow).groupBySource(p => (x = p._1.x).toRow)
      )
    """)
    assert(error.contains(expectedError), s"Expected substring '$expectedError' in '$error'")
  }
}

// T9: Disabling a constraint allows previously-rejected code.
// NonLinear allows the same self-join that Linear rejects (from T8).
class DisablingLinearAllowsSelfJoinTest extends munit.FunSuite {
  test("Disabling linearity allows non-linear self-join") {
    import tyql.*
    import Query.fix

    type Edge = (x: Int, y: Int)
    val base = Table[Edge]("edges")

    // This should compile — NonLinear allows the self-join
    base.fix((constructorFreedom = NonRestrictedConstructors(), monotonicity = NonMonotone(), category = SetResult(), linearity = NonLinear(), mutual = NoMutual()))(pathRec =>
      pathRec.flatMap(p =>
        pathRec.filter(e => p.y == e.x).map(e => (x = p.x, y = e.y).toRow)
      ).distinct
    )
  }
}

// T10: Disabling a constraint allows previously-rejected code.
// AllowMutual allows mutual recursion that NoMutual rejects (from T6).
class DisablingMutualAllowsMutualRecTest extends munit.FunSuite {
  test("Disabling mutual restriction allows mutual recursion") {
    import tyql.*
    import Query.fix

    type Edge = (x: Int, y: Int)
    val base1 = Table[Edge]("edges1")
    val base2 = Table[Edge]("edges2")

    // This should compile — AllowMutual allows mutual recursion
    fix((constructorFreedom = NonRestrictedConstructors(), monotonicity = NonMonotone(), category = BagResult(), linearity = NonLinear(), mutual = AllowMutual()))(base1, base2)((r1, r2) =>
      val p1 = r1.flatMap(p => r2.filter(e => p.y == e.x).map(e => (x = p.x, y = e.y).toRow))
      val p2 = r2.map(e => (x = e.x, y = e.y).toRow)
      (p1, p2)
    )
  }
}
