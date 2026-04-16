package tyql

import scala.annotation.implicitNotFound
import Utils.{GenerateIndices, ZipWithIndex}

// POC of the paper's κ claim (§4.4): each fix invocation is tagged with a
// fresh singleton K.  The dependency tuple D carries `I & K` intersection
// types instead of bare integer indices I, so the existing linearity checks
// (IndexSequence vs UnionDT) naturally reject bodies that smuggle in a
// reference from a different fix invocation.
//
// No new RestrictedQuery / RestrictedQueryRef classes: we just widen
// RestrictedQueryRef's ID bound from `<: Int` to unbounded so it accepts
// both bare ints (non-κ, existing code) and intersection types `I & K`
// (κ-tagged code).

object FixWithK:
  import RestrictedQuery.{Elems, ToQuery}

  /** Ref-tuple: one RestrictedQueryRef per base query, with ID = `index & K`
   *  so the dep tuple carries the κ tag. */
  type ToKRef[QT <: Tuple, K, RCF <: ConstructorFreedom, RM <: MonotoneRestriction] =
    Tuple.Map[ZipWithIndex[Elems[QT]], [T] =>> T match
      case (elem, idx) => RestrictedQueryRef[elem, SetResult, idx & K, RCF, RM]]

  /** κ-decorated index sequence: `{0 & K, 1 & K, …, (n-1) & K}`. */
  type IndexSequenceK[QT <: Tuple, K] =
    Tuple.Union[Tuple.Map[GenerateIndices[0, Tuple.Size[QT]], [I] =>> I & K]]

  /** Union of all D elements across a returned tuple of RestrictedQuery. */
  type UnionDT[RQT <: Tuple] =
    Tuple.Union[Tuple.FlatMap[RQT, RestrictedQuery.ExtractDependencies]]

  // Reuse existing helpers:
  type CheckLinearRelevanceK[RL <: LinearRestriction, QT <: Tuple, K, RQT <: Tuple] = RL match
    case Linear => IndexSequenceK[QT, K] =:= UnionDT[RQT]
    case NonLinear => DummyImplicit

  /** The per-relation D tuple for a Linear fix with κ: `(Tuple1[0 & K], Tuple1[1 & K], …)`. */
  type LinearDTK[QT <: Tuple, K] =
    Tuple.Map[ZipWithIndex[Elems[QT]], [T] =>> T match
      case (_, idx) => Tuple1[idx & K]]

  // -------- Ergonomic wrappers (polymorphic function, scope value) --------

  /** Polymorphic-function wrapper.  Call site:
   *  {{{
   *  FixWithK.restrictedFixPoly(Tuple1(edges)) { [K <: Singleton] => refs =>
   *    val path = refs._1
   *    Tuple1(path.flatMap(…).distinct)
   *  }
   *  }}} */
  def restrictedFixPoly[QT <: Tuple]
    (bases: QT)
    (f: [K] =>
          ToKRef[QT, K, RestrictedConstructors, MonotoneRestriction] =>
          RestrictedQuery.ToRestrictedQuery[QT, LinearDTK[QT, K], RestrictedConstructors, MonotoneRestriction, SetResult])
    (using @implicitNotFound("Base cases must be of type Query: ${QT}")
      ev1: Query.IsTupleOfQueries[QT])
    (using @implicitNotFound("Row types must be NamedTuples: ${QT}")
      evRowTypes: Query.AllRowTypesAreNamedTuples[QT])
    (using @implicitNotFound("restrictedFix does not support mutual recursion")
      evMutual: Tuple.Size[QT] =:= 1)
  : RestrictedQuery.ToQuery[QT] =
    Query.fixImpl(true)(bases)(f[Singleton].asInstanceOf[Nothing => Tuple])
      .asInstanceOf[RestrictedQuery.ToQuery[QT]]

  /** Scope-value (path-dependent) wrapper.  Call site:
   *  {{{
   *  FixWithK.restrictedFixScope(Tuple1(edges)) { scope => refs =>
   *    val path = refs._1
   *    Tuple1(path.flatMap(…).distinct)
   *  }
   *  }}} */
  final class Scope
  def restrictedFixScope[QT <: Tuple]
    (bases: QT)
    (f: (scope: Scope) =>
          ToKRef[QT, scope.type, RestrictedConstructors, MonotoneRestriction] =>
          RestrictedQuery.ToRestrictedQuery[QT, LinearDTK[QT, scope.type], RestrictedConstructors, MonotoneRestriction, SetResult])
    (using @implicitNotFound("Base cases must be of type Query: ${QT}")
      ev1: Query.IsTupleOfQueries[QT])
    (using @implicitNotFound("Row types must be NamedTuples: ${QT}")
      evRowTypes: Query.AllRowTypesAreNamedTuples[QT])
    (using @implicitNotFound("restrictedFix does not support mutual recursion")
      evMutual: Tuple.Size[QT] =:= 1)
  : RestrictedQuery.ToQuery[QT] =
    val scope: Scope = new Scope
    Query.fixImpl(true)(bases)(f(scope).asInstanceOf[Nothing => Tuple])
      .asInstanceOf[RestrictedQuery.ToQuery[QT]]

  // -------- K-explicit methods (verbose but full control) --------

  /** Tuple-based `fix` with κ.  K is a method-level type parameter.
   *  Mirrors `Query.fix` on the companion + the κ relevance check (`evK`). */
  def fix[
    K <: Singleton,
    QT <: Tuple, RQT <: Tuple,
    RCF <: ConstructorFreedom, RM <: MonotoneRestriction, RC <: ResultCategory,
    RL <: LinearRestriction, MR <: MutualRestriction
  ](options: (constructorFreedom: RCF, monotonicity: RM, category: RC, linearity: RL, mutual: MR))
    (q: QT)
    (f: ToKRef[QT, K, RCF, RM] => RQT)
    (using @implicitNotFound("Base cases must be of type Query: ${QT}")
      ev1: Query.IsTupleOfQueries[QT])
    (using @implicitNotFound("Row types must be NamedTuples: ${QT}")
      evRowTypes: Query.AllRowTypesAreNamedTuples[QT])
    (using @implicitNotFound("Number of base cases must match the number of recursive definitions")
      ev0: Tuple.Size[QT] =:= Tuple.Size[RQT])
    (using @implicitNotFound("Mutual recursion is not allowed under the current options")
      evMutual: Query.CheckMutual[MR, QT])
    (using @implicitNotFound("Recursive definitions do not have the expected shape: ${RQT}")
      ev3: RQT <:< RestrictedQuery.ToRestrictedQuery[QT, RestrictedQuery.ToDependencyTuple[RL, RQT], RCF, RM, RC])
    (using @implicitNotFound("Recursive definitions must be linear with matching κ: ${RQT}")
      evK: CheckLinearRelevanceK[RL, QT, K, RQT])
    (using @implicitNotFound("Recursive references cannot appear twice within the same definition")
      ev5: Query.CheckLinearDuplicates[RL, RQT])
  : ToQuery[QT] =
    Query.fixImpl(options.category.isInstanceOf[SetResult])(q)(f)

  /** Tuple-based `restrictedFix` with κ.  K is a method-level type parameter.
   *  Mirrors `Query.restrictedFix` on the companion. */
  def restrictedFix[K <: Singleton, QT <: Tuple, RQT <: Tuple]
    (q: QT)
    (f: ToKRef[QT, K, RestrictedConstructors, MonotoneRestriction] => RQT)
    (using @implicitNotFound("Base cases must be of type Query: ${QT}")
      ev1: Query.IsTupleOfQueries[QT])
    (using @implicitNotFound("Row types must be NamedTuples: ${QT}")
      evRowTypes: Query.AllRowTypesAreNamedTuples[QT])
    (using @implicitNotFound("Number of base cases must match the number of recursive definitions")
      ev0: Tuple.Size[QT] =:= Tuple.Size[RQT])
    (using @implicitNotFound("restrictedFix does not support mutual recursion")
      evMutual: Tuple.Size[QT] =:= 1)
    (using @implicitNotFound("Recursive definitions do not have the expected shape: ${RQT}")
      ev3: RQT <:< RestrictedQuery.ToRestrictedQuery[QT, RestrictedQuery.InverseMapDeps[RQT], RestrictedConstructors, MonotoneRestriction, SetResult])
    (using @implicitNotFound("Recursive definitions must be linear with matching κ: ${RQT}")
      evK: IndexSequenceK[QT, K] =:= UnionDT[RQT])
    (using @implicitNotFound("Recursive references cannot appear twice within the same definition")
      ev5: Query.NoDuplicateReferences[RQT])
  : ToQuery[QT] =
    Query.fixImpl(true)(q)(f)
