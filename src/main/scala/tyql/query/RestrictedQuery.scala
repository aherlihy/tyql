package tyql

import tyql.Utils.{GenerateIndices, HasDuplicate, ZipWithIndex}
import tyql.{DatabaseAST, Expr, NonScalarExpr, Query, ResultTag}

import scala.NamedTuple.{AnyNamedTuple, NamedTuple}
import scala.annotation.{implicitNotFound, targetName}

trait MonotoneRestriction
class Monotone extends MonotoneRestriction
class NonMonotone extends MonotoneRestriction

trait LinearRestriction
class Linear extends LinearRestriction
class NonLinear extends LinearRestriction

trait MutualRestriction
class NoMutual extends MutualRestriction
class AllowMutual extends MutualRestriction

case class RestrictedQueryRef[A: ResultTag, C <: ResultCategory, ID, RestrictionCF <: ConstructorFreedom, RestrictionM <: MonotoneRestriction](w: Option[Query.QueryRef[A, C]] = None) extends RestrictedQuery[A, C, Tuple1[ID], RestrictionCF, RestrictionM] (w.getOrElse(Query.QueryRef[A, C]())):
  type Self = this.type
  def toQueryRef: Query.QueryRef[A, C] = wrapped.asInstanceOf[Query.QueryRef[A, C]]

/**
 * A restricted reference to a query that disallows aggregation.
 * Explicitly do not export aggregate, or any aggregation helpers, exists, etc.
 *
 * Methods can accept RestrictedQuery[A] or Query[A]
 * NOTE: Query[?] indicates no aggregation, but could turn into aggregation, RestrictedQuery[?] means none present and none addable.
 *
 * flatMap/union/unionAll/etc. that accept another RestrictedQuery contain a contextual parameter ev that serves as an affine
 * recursion restriction, e.g. every input relation can only be "depended" on at most once per query since the dependency set
 * parameter `D` must be disjoint.
 */
class RestrictedQuery[A, C <: ResultCategory, D <: Tuple, RestrictionCF <: ConstructorFreedom, RestrictionM <: MonotoneRestriction](using ResultTag[A])(protected val wrapped: Query[A, C]) extends DatabaseAST[A]:
  val tag: ResultTag[A] = qTag
  type deps
  def toQuery: Query[A, C] = wrapped

  /** Aggregate returning AggregationExpr (single-level: sum, min, toGroupingRow).
   *  Only available when monotonicity is disabled (NonMonotone). */
  def aggregate[B: ResultTag]
    (f: Expr.Ref[A, NonScalarExpr, RestrictionCF] => AggregationExpr[B])
    (using RestrictionM =:= NonMonotone)
  : RestrictedQuery.RestrictedAggregation[A *: EmptyTuple, B, D, RestrictionCF] =
    val ref = Expr.Ref[A, NonScalarExpr, RestrictionCF]()
    val agg = Aggregation.AggFlatMap[A *: EmptyTuple, B](wrapped, Expr.Fun(ref, f(ref)))
    RestrictedQuery.RestrictedAggregation[A *: EmptyTuple, B, D, RestrictionCF](agg)

  /** Aggregate returning a named-tuple of mixed Expr/AggregationExpr (no .toGroupingRow needed).
   *  Only available when monotonicity is disabled (NonMonotone). */
  @targetName("restrictedAggregateNamedTuple")
  def aggregate[B <: AnyNamedTuple: Expr.IsTupleOfMixedExpr]
    (using ResultTag[NamedTuple.Map[B, Expr.StripExpr]])
    (f: Expr.Ref[A, NonScalarExpr, RestrictionCF] => B)
    (using RestrictionM =:= NonMonotone)
  : RestrictedQuery.RestrictedAggregation[A *: EmptyTuple, NamedTuple.Map[B, Expr.StripExpr], D, RestrictionCF] =
    val ref = Expr.Ref[A, NonScalarExpr, RestrictionCF]()
    val row = AggregationExpr.AggProject(f(ref))
    val agg = Aggregation.AggFlatMap[A *: EmptyTuple, NamedTuple.Map[B, Expr.StripExpr]](wrapped, Expr.Fun(ref, row))
    RestrictedQuery.RestrictedAggregation[A *: EmptyTuple, NamedTuple.Map[B, Expr.StripExpr], D, RestrictionCF](agg)

  /** Nested aggregate: f returns a RestrictedAggregation (from inner restricted .aggregate).
   *  Source types accumulate. Dependencies concatenated. */
  @targetName("restrictedAggregateNestedRestricted")
  def aggregate[B: ResultTag, T <: Tuple, D2 <: Tuple]
    (f: Expr.Ref[A, NonScalarExpr, RestrictionCF] => RestrictedQuery.RestrictedAggregation[T, B, D2, RestrictionCF])
    (using RestrictionM =:= NonMonotone)
  : RestrictedQuery.RestrictedAggregation[A *: T, B, Tuple.Concat[D, D2], RestrictionCF] =
    val ref = Expr.Ref[A, NonScalarExpr, RestrictionCF]()
    val unwrapped: Expr.Ref[A, NonScalarExpr, RestrictionCF] => Aggregation[T, B] = r => f(r).wrapped
    val agg = Aggregation.AggFlatMap[A *: T, B](wrapped, Expr.Fun(ref, unwrapped(ref)))
    RestrictedQuery.RestrictedAggregation[A *: T, B, Tuple.Concat[D, D2], RestrictionCF](agg)

  /** Nested aggregate: f returns a plain Aggregation (from non-recursive Query.aggregate).
   *  No new dependencies added. Source types accumulate. */
  @targetName("restrictedAggregateNestedPlain")
  def aggregate[B: ResultTag, T <: Tuple]
    (f: Expr.Ref[A, NonScalarExpr, RestrictionCF] => Aggregation[T, B])
    (using RestrictionM =:= NonMonotone)
  : RestrictedQuery.RestrictedAggregation[A *: T, B, D, RestrictionCF] =
    val ref = Expr.Ref[A, NonScalarExpr, RestrictionCF]()
    val agg = Aggregation.AggFlatMap[A *: T, B](wrapped, Expr.Fun(ref, f(ref)))
    RestrictedQuery.RestrictedAggregation[A *: T, B, D, RestrictionCF](agg)

  // flatMap given a function that returns regular Query does not add any dependencies
  @targetName("restrictedQueryFlatMap")
  def flatMap[B: ResultTag](f: Expr.Ref[A, NonScalarExpr, RestrictionCF] => Query[B, ?]): RestrictedQuery[B, BagResult, D, RestrictionCF, RestrictionM] =
    val ref = Expr.Ref[A, NonScalarExpr, RestrictionCF]()
    RestrictedQuery(Query.FlatMap(wrapped, Expr.Fun(ref, f(ref))))

  @targetName("restrictedQueryFlatMapRestricted")
  def flatMap[B: ResultTag, D2 <: Tuple](f: Expr.Ref[A, NonScalarExpr, RestrictionCF] => RestrictedQuery[B, ?, D2, RestrictionCF, RestrictionM]): RestrictedQuery[B, BagResult, Tuple.Concat[D, D2], RestrictionCF, RestrictionM] =
//  (using @implicitNotFound("Recursive definition must be linearly recursive, e.g. each recursive reference cannot be used twice") ev: Tuple.Disjoint[D, D2] =:= true)
    val toR: Expr.Ref[A, NonScalarExpr, RestrictionCF] => Query[B, ?] = arg => f(arg).toQuery
    val ref = Expr.Ref[A, NonScalarExpr, RestrictionCF]()
    RestrictedQuery(Query.FlatMap(wrapped, Expr.Fun(ref, toR(ref))))

  def map[B: ResultTag, CF <: ConstructorFreedom](f: Expr.Ref[A, NonScalarExpr, RestrictionCF] => Expr[B, NonScalarExpr, CF]): RestrictedQuery[B, BagResult, D, RestrictionCF, RestrictionM] =
    val ref = Expr.Ref[A, NonScalarExpr, RestrictionCF]()
    RestrictedQuery(Query.Map(wrapped, Expr.Fun(ref, f(ref))))

  def map[B <: AnyNamedTuple: Expr.IsTupleOfExpr](using ResultTag[NamedTuple.Map[B, Expr.StripExpr]])(f: Expr.Ref[A, NonScalarExpr, RestrictionCF] => B): RestrictedQuery[NamedTuple.Map[B, Expr.StripExpr], BagResult, D, RestrictionCF, RestrictionM] =
    import Expr.toRow
    val ref = Expr.Ref[A, NonScalarExpr, RestrictionCF]()
    RestrictedQuery(Query.Map(wrapped, Expr.Fun(ref, f(ref).toRow)))

  def withFilter[CF <: ConstructorFreedom](p: Expr.Ref[A, NonScalarExpr, RestrictionCF] => Expr[Boolean, NonScalarExpr, CF]): RestrictedQuery[A, C, D, RestrictionCF, RestrictionM] =
    val ref = Expr.Ref[A, NonScalarExpr, RestrictionCF]()
    RestrictedQuery(Query.Filter[A, C](wrapped, Expr.Fun(ref, p(ref))))
  def filter[CF <: ConstructorFreedom](p: Expr.Ref[A, NonScalarExpr, RestrictionCF] => Expr[Boolean, NonScalarExpr, CF]): RestrictedQuery[A, C, D, RestrictionCF, RestrictionM] =
    val ref = Expr.Ref[A, NonScalarExpr, RestrictionCF]()
    RestrictedQuery(Query.Filter[A, C](wrapped, Expr.Fun(ref, p(ref))))

  def distinct: RestrictedQuery[A, SetResult, D, RestrictionCF, RestrictionM] = RestrictedQuery(wrapped.distinct)

  def union[D2 <: Tuple](that: RestrictedQuery[A, ?, D2, RestrictionCF, RestrictionM])
//                        (using @implicitNotFound("Recursive definition must be linearly recursive, e.g. each recursive reference cannot be used twice") ev: Tuple.Disjoint[D, D2] =:= true)
    : RestrictedQuery[A, SetResult, Tuple.Concat[D, D2], RestrictionCF, RestrictionM] =
    RestrictedQuery(Query.Union(wrapped, that.toQuery))

  def unionAll[D2 <: Tuple](that: RestrictedQuery[A, ?, D2, RestrictionCF, RestrictionM])
//                           (using @implicitNotFound("Recursive definition must be linearly recursive, e.g. each recursive reference cannot be used twice") ev: Tuple.Disjoint[D, D2] =:= true)
    : RestrictedQuery[A, BagResult, Tuple.Concat[D, D2], RestrictionCF, RestrictionM] =
    RestrictedQuery(Query.UnionAll(wrapped, that.toQuery))

  @targetName("unionQuery")
  def union(that: Query[A, ?]): RestrictedQuery[A, SetResult, D, RestrictionCF, RestrictionM] =
    RestrictedQuery(Query.Union(wrapped, that))
  @targetName("unionAllQuery")
  def unionAll(that: Query[A, ?]): RestrictedQuery[A, BagResult, D, RestrictionCF, RestrictionM] =
    RestrictedQuery(Query.UnionAll(wrapped, that))

  // TODO: Does nonEmpty count as non-monotone? (yes)
  def nonEmpty: Expr[Boolean, NonScalarExpr, RestrictionCF] =
    Expr.NonEmpty(wrapped)

  def isEmpty: Expr[Boolean, NonScalarExpr, RestrictionCF] =
    Expr.IsEmpty(wrapped)

object RestrictedQuery {

  /** Given a Tuple `(Query[A], Query[B], ...)`, return `(A, B, ...)` */
  type Elems[QT <: Tuple] = Tuple.InverseMap[QT, [T] =>> Query[T, ?]]

  /**
   * Given a Tuple `(Query[A], Query[B], ...)`, return `(Query[A], Query[B], ...)`
   *
   *  This isn't just the identity because the input might actually be a subtype e.g.
   *  `(Table[A], Table[B], ...)`
   */
  type ToQuery[QT <: Tuple] = Tuple.Map[Elems[QT], [T] =>> Query.MultiRecursive[T]]

  type ConstructRestrictedQuery[T, CF <: ConstructorFreedom, RM <: MonotoneRestriction, RC <: ResultCategory] = T match
    case (t, d) => RestrictedQuery[t, RC, d, CF, RM]

  type ToRestrictedQuery[QT <: Tuple, DT <: Tuple, RCF <: ConstructorFreedom, RM <: MonotoneRestriction, RC <: ResultCategory] =
    Tuple.Map[Tuple.Zip[Elems[QT], DT], [T] =>> ConstructRestrictedQuery[T, RCF, RM, RC]]
  type ToRestrictedQueryRef[QT <: Tuple, K, RCF <: ConstructorFreedom, RM <: MonotoneRestriction] = Tuple.Map[ZipWithIndex[Elems[QT]], [T] =>> T match
    case (elem, index) => RestrictedQueryRef[elem, SetResult, index & K, RCF, RM]]

  // Adjustable restrictions:
  // only include the monotone restriction, ignore category or linearity
  type ToMonotoneQuery[QT <: Tuple] = Tuple.Map[Elems[QT], [T] =>> RestrictedQuery[T, ?, ?, NonRestrictedConstructors, MonotoneRestriction]]
  type ToMonotoneQueryRef[QT <: Tuple] = Tuple.Map[Elems[QT], [T] =>> RestrictedQueryRef[T, ?, ?, NonRestrictedConstructors, MonotoneRestriction]]

  // ignore all restrictions
  type ToUnrestrictedQuery[QT <: Tuple] = Tuple.Map[Elems[QT], [T] =>> Query[T, ?]]
  type ToUnrestrictedQueryRef[QT <: Tuple] = Tuple.Map[Elems[QT], [T] =>> Query.QueryRef[T, ?]]

  /**
   * Check if the query is relevant, e.g. all dependencies are used at least once in all result queries
   */
  type CheckRelevantQuery[RL <: LinearRestriction, QT <: Tuple, RQT <: Tuple] = RL match
    case Linear => ExpectedResult[QT] <:< ActualResult[RQT]
    case NonLinear => true

  /**
   * Extract the dependencies from a tuple of restricted queries and optionally check for duplicates.
   */
  type ToDependencyTuple[RL <: LinearRestriction, RQT <: Tuple] <: Tuple = RL match
    case Linear => InverseMapDeps[RQT]
    case NonLinear => NonlinearInverseMapDeps[RQT]

  /**
   * Extract the dependencies from a tuple of restricted queries.
   * Returns a tuple of dependencies for each element in the tuple, or nothing if there are duplicates.
   */
  type InverseMapDeps[RQT <: Tuple] <: Tuple = RQT match {
    case RestrictedQuery[a, c, d, rcf, mf] *: t => HasDuplicate[d] *: InverseMapDeps[t]
    case EmptyTuple => EmptyTuple
  }

  /**
   * Same as InverseMapDeps except it does not check for duplicates.
   */
  type NonlinearInverseMapDeps[RQT <: Tuple] <: Tuple = RQT match {
    case RestrictedQuery[a, c, d, rcf, mf] *: t => d *: NonlinearInverseMapDeps[t]
    case EmptyTuple => EmptyTuple
  }

    // test affine
//    type testA = (0, 1)
//    val checkA = summon[testA =:= HasDuplicate[testA]]
//    val testB = ((0, 1), (0, 3), (1, 2))
//    val checkB = summon[testB =:= HasDuplicate[testB]]

  // test linear
//    type Actual = Tuple3[Tuple1[0],Tuple1[0],Tuple1[1]]
//    type FlatActual = Tuple.FlatMap[Actual, [T <: Tuple] =>> T]
//    type Expected = GenerateIndices[0, Tuple.Size[(0,0,0)]]
//    type UnionActual = Tuple.Union[FlatActual]
//    type UnionExpected = Tuple.Union[Expected]
//    val check: UnionExpected <:< UnionActual = summon[UnionExpected <:< UnionActual]

  // Checks to ensure that no dependencies are missing from all results (e.g. relevant)
  type ExtractDependencies[D] <: Tuple = D match
    case RestrictedQuery[a, c, d, rcf, mf] => d
  type ExpectedResult[QT <: Tuple] = Tuple.Union[GenerateIndices[0, Tuple.Size[QT]]]
  type ActualResult[RT <: Tuple] = Tuple.Union[Tuple.FlatMap[RT, ExtractDependencies]]

  /**
   * Restricted aggregation: wraps an Aggregation while preserving dependency tracking (D) and
   * constructor freedom (RCF). Only producible from RestrictedQuery[..., NonMonotone].aggregate(...).
   * Provides groupBySource which returns a RestrictedQuery, keeping all constraints enforceable.
   */
  class RestrictedAggregation[AllSourceTypes <: Tuple, Result, D <: Tuple, RCF <: ConstructorFreedom]
    (using ResultTag[Result])(val wrapped: Aggregation[AllSourceTypes, Result]):

    def groupBySource[GroupResult, GroupShape <: ExprShape]
      (groupingFn: ToNonScalarRef[AllSourceTypes] => Expr[GroupResult, GroupShape, ?])
    : RestrictedQuery[Result, BagResult, D, RCF, NonMonotone] =
      RestrictedQuery(wrapped.groupBySource(groupingFn))

    @targetName("groupBySourceNamedTuple")
    def groupBySource[B <: AnyNamedTuple: Expr.IsTupleOfExpr]
      (using rt: ResultTag[NamedTuple.Map[B, Expr.StripExpr]])
      (groupingFn: ToNonScalarRef[AllSourceTypes] => B)
    : RestrictedQuery[Result, BagResult, D, RCF, NonMonotone] =
      import Expr.toRow
      groupBySource[NamedTuple.Map[B, Expr.StripExpr], NonScalarExpr](r => groupingFn(r).toRow)

}
