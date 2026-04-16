package tyql

import language.experimental.namedTuples
import NamedTuple.{AnyNamedTuple, NamedTuple}
import scala.annotation.{implicitNotFound, targetName}
import scala.reflect.ClassTag
import Utils.{GenerateIndices, HasDuplicate, naturalMap}
import tyql.Expr.{IsTupleOfExpr, StripExpr}

trait ResultCategory
class SetResult extends ResultCategory
class BagResult extends ResultCategory

trait Query[A, Category <: ResultCategory](using ResultTag[A]) extends DatabaseAST[A]:
  import Expr.{Fun, Ref}
  val tag: ResultTag[A] = qTag
  /**
   * Classic flatMap with an inner Query that will likely be flattened into a join.
   *
   * @param f    a function that returns a Query (so no aggregations in the subtree)
   * @tparam B   the result type of the query.
   * @return     Query[B], e.g. an iterable of results of type B
   */
  def flatMap[B: ResultTag](f: Ref[A, NonScalarExpr, NonRestrictedConstructors] => Query[B, ?]): Query[B, BagResult] =
    val ref = Ref[A, NonScalarExpr, NonRestrictedConstructors]()
    Query.FlatMap(this, Fun(ref, f(ref)))

  /**
   * Classic flatMap with an inner query that is a RestrictedQuery.
   * This turns the result query into a RestrictedQuery.
   * Exists to support doing BaseCaseRelation.flatMap(...) within a fix
   * @param f   a function that returns a RestrictedQuery, meaning it has used a recursive definition from fix.
   * @tparam B  the result type of the query.
   * @return    RestrictedQuery[B]
   */
  def flatMap[B: ResultTag, D <: Tuple, RCF <: ConstructorFreedom, MR <: MonotoneRestriction](f: Ref[A, NonScalarExpr, NonRestrictedConstructors] => RestrictedQuery[B, ?, D, RCF, MR]): RestrictedQuery[B, BagResult, D, RCF, MR] =
    val ref = Ref[A, NonScalarExpr, NonRestrictedConstructors]()
    RestrictedQuery(Query.FlatMap(this, Fun(ref, f(ref).toQuery)))
  /**
   * Equivalent to flatMap(f: Ref => Aggregation).
   * NOTE: make Ref of type NExpr so that relation.id is counted as NExpr, not ScalarExpr
   *
   * @param f   a function that returns an Aggregation (guaranteed agg in subtree)
   * @tparam B  the result type of the aggregation.
   * @return    Aggregation[B], a scalar result, e.g. a single value of type B.
   */
  def aggregate[B: ResultTag, T <: Tuple](f: Ref[A, NonScalarExpr, NonRestrictedConstructors] => Aggregation[T, B]): Aggregation[A *: T, B] =
    val ref = Ref[A, NonScalarExpr, NonRestrictedConstructors]()
    Aggregation.AggFlatMap[A *: T, B](this, Fun(ref, f(ref)))

  /** Aggregate where body returns a RestrictedAggregation (from a restricted inner aggregate).
   *  Follows the same pattern as flatMap(Ref => RestrictedQuery): unwraps and preserves deps.
   */
  @targetName("AggregateRestrictedAgg")
  def aggregate[B: ResultTag, T <: Tuple, D <: Tuple, RCF <: ConstructorFreedom]
    (f: Ref[A, NonScalarExpr, NonRestrictedConstructors] => RestrictedQuery.RestrictedAggregation[T, B, D, RCF])
  : RestrictedQuery.RestrictedAggregation[A *: T, B, D, RCF] =
    val ref = Ref[A, NonScalarExpr, NonRestrictedConstructors]()
    val unwrapped: Ref[A, NonScalarExpr, NonRestrictedConstructors] => Aggregation[T, B] = r => f(r).wrapped
    val agg = Aggregation.AggFlatMap[A *: T, B](this, Fun(ref, unwrapped(ref)))
    RestrictedQuery.RestrictedAggregation[A *: T, B, D, RCF](agg)

  /**
   * Equivalent version of map(f: Ref => AggregationExpr).
   * Requires f to call toRow on the final result before returning.
   * Sometimes the implicit conversion kicks in and converts a named-tuple-of-Agg into a Agg-of-named-tuple,
   * but not always. As an alternative, can use the aggregate defined below that explicitly calls toRow on the result of f.
   *
   * @param f   a function that returns an Aggregation (guaranteed agg in subtree)
   * @tparam B  the result type of the aggregation.
   * @return    Aggregation[B], a scalar result, e.g. single value of type B.
   */
  @targetName("AggregateExpr")
  def aggregate[B: ResultTag](f: Ref[A, NonScalarExpr, NonRestrictedConstructors] => AggregationExpr[B]): Aggregation[A *: EmptyTuple, B] =
    val ref = Ref[A, NonScalarExpr, NonRestrictedConstructors]()
    Aggregation.AggFlatMap[A *: EmptyTuple, B](this, Fun(ref, f(ref)))

  /**
   * A version of the above-defined aggregate that allows users to skip calling toRow on the result in f.
   *
   * @param f    a function that returns a named-tuple-of-Aggregation.
   * @tparam B   the named-tuple-of-Aggregation that will be converted to an Aggregation-of-named-tuple
   * @return     Aggregation of B.toRow, e.g. a scalar result of type B.toRow
   */
  def aggregate[B <: AnyNamedTuple: AggregationExpr.IsTupleOfAgg]/*(using ev: AggregationExpr.IsTupleOfAgg[B] =:= true)*/
    (using ResultTag[NamedTuple.Map[B, Expr.StripExpr]])
    (f: Ref[A, NonScalarExpr, NonRestrictedConstructors] => B)
  : Aggregation[A *: EmptyTuple, NamedTuple.Map[B, Expr.StripExpr] ] =

    import AggregationExpr.toRow
    val ref = Ref[A, NonScalarExpr, NonRestrictedConstructors]()
    val row = f(ref).toRow
    Aggregation.AggFlatMap[A *: EmptyTuple, NamedTuple.Map[B, Expr.StripExpr] ](this, Fun(ref, row.asInstanceOf[Expr[NamedTuple.Map[B, Expr.StripExpr], ScalarExpr, NonRestrictedConstructors]]))

//  inline def aggregate[B: ResultTag](f: Ref[A, ScalarExpr] => Query[B]): Nothing =
//    error("No aggregation function found in f. Did you mean to use flatMap?")

// TODO: Restrictions for groupBy: all columns in the selectFn must either be in the groupingFn or in an aggregate.
//  type GetFields[T] <: Tuple = T match
//    case Expr[t, ?] => GetFields[t]
//    case NamedTuple[n, v] => n
// TODO: figure out how to do groupBy on join result.
//  GroupBy grouping function when applied to the result of a join accesses only columns from the original queries.
// TODO: Right now groupBy most closely resembles SQL groupBy, not Spark RDD's or pairs.
//   Do we want to pick one?
// TODO: Merge groupBy, groupByAggregate, and filterByGroupBy?
//  Right now separated due to issues with overloading, but in theory could be condensed into a single groupBy method
  /**
   * groupBy where the grouping clause is NOT an aggregation.
   * Can add a 'having' statement incrementally by calling .having on the result.
   * The selectFn MUST return an aggregation.
   *
   * @param groupingFn - must be a named tuple, in order from left->right. Must return an non-scalar expression.
   * @param selectFn - the project clause of the select statement that is grouped
   * NOTE: filterFn - the HAVING clause is used to filter groups after the GROUP BY operation has been applied. filters
   * applied before the groupBy occur in the WHERE clause.
   * @tparam R - the return type of the expression
   * @tparam GroupResult - the type of the grouping statement
   * @return
   */
  def groupBy[R: ResultTag, GroupResult](
                                          groupingFn: Ref[A, NonScalarExpr, NonRestrictedConstructors] => Expr[GroupResult, NonScalarExpr, NonRestrictedConstructors],
                                          selectFn: Ref[A, ScalarExpr, NonRestrictedConstructors] => Expr[R, ScalarExpr, NonRestrictedConstructors]
                                          //  (using ev: Tuple.Union[GetFields[A]] <:< Tuple.Union[GetFields[G]])
   ): Query.GroupBy[A, R, GroupResult, NonScalarExpr, ScalarExpr] =
    val refG = Ref[A, NonScalarExpr, NonRestrictedConstructors]()
    val groupFun = Fun(refG, groupingFn(refG))

    val refS = Ref[A, ScalarExpr, NonRestrictedConstructors]()
    val selectFun = Fun(refS, selectFn(refS))
    Query.GroupBy(this, groupFun, selectFun, None)

  /**
   * groupBy where the grouping clause IS an aggregation.
   * Can add a 'having' statement incrementally by calling .having on the result.
   * The selectFn MUST return an aggregation.
   *
   * @param groupingFn - must be a named tuple, in order from left->right. Must return an aggregation.
   * @param selectFn - the project clause of the select statement that is grouped.
   * NOTE: filterFn - the HAVING clause is used to filter groups after the GROUP BY operation has been applied. filters
   * applied before the groupBy occur in the WHERE clause.
   * @tparam R - the return type of the expression
   * @tparam GroupResult - the type of the grouping statement
   * @return
   */
  def groupByAggregate[R: ResultTag, GroupResult](
                                                   groupingFn: Ref[A, ScalarExpr, NonRestrictedConstructors] => Expr[GroupResult, ScalarExpr, NonRestrictedConstructors],
                                                   selectFn: Ref[A, ScalarExpr, NonRestrictedConstructors] => Expr[R, ScalarExpr, NonRestrictedConstructors]
  ): Query.GroupBy[A, R, GroupResult, ScalarExpr, ScalarExpr] =
    val refG = Ref[A, ScalarExpr, NonRestrictedConstructors]()
    val groupFun = Fun(refG, groupingFn(refG))

    val refS = Ref[A, ScalarExpr, NonRestrictedConstructors]()
    val selectFun = Fun(refS, selectFn(refS))
    Query.GroupBy(this, groupFun, selectFun, None)

  /**
   * filter based on a groupBy result.
   * The selectFn MUST NOT return an aggregation.
   * The groupingFn MUST NOT return an aggregation.
   * The filterFn MUST return an aggregation.
   *
   * @param groupingFn - must be a named tuple, in order from left->right. Must return a scalar expression.
   * @param selectFn - the project clause of the select statement that is grouped. Must return a scalar expression.
   * @param havingFn - the filter clause. Must return an aggregation.
   * @tparam R - the return type of the expression
   * @tparam GroupResult - the type of the grouping statement
   * @return
   */
  def filterByGroupBy[R: ResultTag, GroupResult](
                                                  groupingFn: Ref[A, NonScalarExpr, NonRestrictedConstructors] => Expr[GroupResult, NonScalarExpr, NonRestrictedConstructors],
                                                  selectFn: Ref[A, NonScalarExpr, NonRestrictedConstructors] => Expr[R, NonScalarExpr, NonRestrictedConstructors],
                                                  havingFn: Ref[A, ?, NonRestrictedConstructors] => Expr[Boolean, ?, NonRestrictedConstructors]
                                        ): Query.GroupBy[A, R, GroupResult, NonScalarExpr, NonScalarExpr] =
    val refG = Ref[A, NonScalarExpr, NonRestrictedConstructors]()
    val groupFun = Fun(refG, groupingFn(refG))

    val refS = Ref[A, NonScalarExpr, NonRestrictedConstructors]()
    val selectFun = Fun(refS, selectFn(refS))

    // Cast is workaround for: "ScalarExpr is not subtype of ?"
    Query.GroupBy(this, groupFun, selectFun, None).having(havingFn).asInstanceOf[Query.GroupBy[A, R, GroupResult, NonScalarExpr, NonScalarExpr]]

//  def groupByAny[R: ResultTag, GroupResult, GroupShape <: ExprShape](
//    groupingFn: Ref[A, GroupShape] => Expr[GroupResult, GroupShape],
//    selectFn: Ref[A, ScalarExpr] => Expr[R, ScalarExpr]
//  ): Query.GroupBy[A, R, GroupResult, GroupShape] =
//    val refG = Ref[A, GroupShape]()
//    val groupFun = Fun(refG, groupingFn(refG))
//
//    val refS = Ref[A, ScalarExpr]()
//    val selectFun = Fun(refS, selectFn(refS))
//    Query.GroupBy(this, groupFun, selectFun, None)


  /**
   * Classic map with an inner expression to transform the row.
   * Requires f to call toRow on the final result before returning.
   * Sometimes the implicit conversion kicks in and converts a named-tuple-of-Expr into a Expr-of-named-tuple,
   * but not always. As an alternative, can use the map defined below that explicitly calls toRow on the result of f.
   *
   * @param f     a function that returns a Expression.
   * @tparam B    the result type of the Expression, and resulting query.
   * @return      Query[B], an iterable of type B.
   *
   */
  def map[B: ResultTag](f: Ref[A, NonScalarExpr, NonRestrictedConstructors] => Expr[B, NonScalarExpr, ?]): Query[B, BagResult] =
    val ref = Ref[A, NonScalarExpr, NonRestrictedConstructors]()
    Query.Map(this, Fun(ref, f(ref)))

  /**
   * A version of the above-defined map that allows users to skip calling toRow on the result in f.
   *
   * @param f    a function that returns a named-tuple-of-Expr.
   * @tparam B   the named-tuple-of-Expr that will be converted to an Expr-of-named-tuple
   * @return     Expr of B.toRow, e.g. an iterable of type B.toRow
   */
  def map[B <: AnyNamedTuple: IsTupleOfExpr](using ResultTag[NamedTuple.Map[B, Expr.StripExpr]])(f: Ref[A, NonScalarExpr, NonRestrictedConstructors] => B): Query[ NamedTuple.Map[B, Expr.StripExpr], BagResult ] =
    import Expr.toRow
    val ref = Ref[A, NonScalarExpr, NonRestrictedConstructors]()
    Query.Map(this, Fun(ref, f(ref).toRow))

  trait LinearConstraint[RL <: LinearRestriction, DT1 <: Tuple]
  object LinearConstraint:
    given linearOk: LinearConstraint[Linear, Tuple1[0]] with {}
    given nonLinearOk[DT1 <: Tuple]: LinearConstraint[NonLinear, DT1] with {}

  /** Conditionally tag the ref ID with K based on linearity option.
   *  Linear: ref has `0 & K` → κ enforced.
   *  NonLinear: ref has bare `0` → K is a no-op. */
  type FixRefID[RL <: LinearRestriction, K] = RL match
    case Linear    => 0 & K
    case NonLinear => 0

  /** Conditionally constrain the return D based on linearity option.
   *  Linear: D must be `Tuple1[0 & K]` → linearity + κ enforced.
   *  NonLinear: D is free (DT1 inferred from body) → no linearity check. */
  type FixReturnD[RL <: LinearRestriction, K, DT1 <: Tuple] = RL match
    case Linear    => Tuple1[0 & K]
    case NonLinear => DT1

  /** Instance `fix` with κ.  Uses `[K] =>` polymorphic body.
   *  When `linearity = Linear`, κ is enforced via `0 & K` in the dep tuple.
   *  When `linearity = NonLinear`, K is a no-op (ref has bare `0`, D is unconstrained).
   *  All other constraints (monotonicity, category, constructor-freedom) are
   *  always enforced via RestrictedQuery's type parameters. */
  def fix[RCF <: ConstructorFreedom, RM <: MonotoneRestriction, RC <: ResultCategory, RL <: LinearRestriction, MR <: MutualRestriction, DT1 <: Tuple]
    (options: (constructorFreedom: RCF, monotonicity: RM, category: RC, linearity: RL, mutual: MR), materialized: Boolean = false)
    (using A <:< AnyNamedTuple)
    (p: [K] => RestrictedQueryRef[A, ?, FixRefID[RL, K], RCF, RM] => RestrictedQuery[A, RC, FixReturnD[RL, K, DT1], RCF, RM])
    (using @implicitNotFound("Failed to generate recursive queries")
      ev3: Tuple1[RestrictedQuery[A, RC, FixReturnD[RL, Any, DT1], RCF, RM]] <:< RestrictedQuery.ToRestrictedQuery[Tuple1[Query[A, ?]], RestrictedQuery.ToDependencyTuple[RL, Tuple1[RestrictedQuery[A, RC, FixReturnD[RL, Any, DT1], RCF, RM]]], RCF, RM, RC])
    (using @implicitNotFound("Recursive definitions must be linear, e.g. recursive references must appear at least once in all the recursive definitions")
      ev4: Query.CheckLinearRelevance[RL, Tuple1[Query[A, ?]], Tuple1[RestrictedQuery[A, RC, FixReturnD[RL, Any, DT1], RCF, RM]]])
    (using @implicitNotFound("Recursive definitions must be linear, e.g. recursive references cannot appear twice within the same recursive definition")
      ev5: Query.CheckLinearDuplicates[RL, Tuple1[RestrictedQuery[A, RC, FixReturnD[RL, Any, DT1], RCF, RM]]])
  : Query[A, BagResult] =
    val fn: Any => Any = { refs =>
      val ref = refs.asInstanceOf[Product].productElement(0)
      Tuple1(p[Singleton](ref.asInstanceOf))
    }
    Query.fixImpl(options.category.isInstanceOf[SetResult], materialized)(
      Tuple1(this))(fn.asInstanceOf[Nothing => Tuple])
      .asInstanceOf[Tuple1[Query.MultiRecursive[A]]]._1

  /** Instance `restrictedFix` with κ.  Always Linear, always κ-enforced. */
  def restrictedFix
    (using A <:< AnyNamedTuple)
    (p: [K] => RestrictedQueryRef[A, ?, 0 & K, RestrictedConstructors, MonotoneRestriction] => RestrictedQuery[A, SetResult, Tuple1[0 & K], RestrictedConstructors, MonotoneRestriction])
  : Query[A, BagResult] =
    val fn: Any => Any = { refs =>
      val ref = refs.asInstanceOf[Product].productElement(0)
      Tuple1(p[Singleton](ref.asInstanceOf))
    }
    Query.fixImpl(true)(Tuple1(this))(fn.asInstanceOf[Nothing => Tuple])
      .asInstanceOf[Tuple1[Query.MultiRecursive[A]]]._1



  def unrestrictedFix(p: Query.QueryRef[A, ?] => Query[A, ?]): Query[A, BagResult] =
    type QT = Tuple1[Query[A, ?]]
    val fn: Tuple1[Query.QueryRef[A, ?]] => Tuple1[Query[A, ?]] = r => Tuple1(p(r._1))
    Query.unrestrictedFix[QT](Tuple1(this))(fn)._1

  def unrestrictedBagFix(p: Query.QueryRef[A, ?] => Query[A, ?]): Query[A, BagResult] =
    type QT = Tuple1[Query[A, ?]]
    val fn: Tuple1[Query.QueryRef[A, ?]] => Tuple1[Query[A, ?]] = r => Tuple1(p(r._1))
    Query.unrestrictedBagFix[QT](Tuple1(this))(fn)._1

  def withFilter(p: Ref[A, NonScalarExpr, NonRestrictedConstructors] => Expr[Boolean, NonScalarExpr, ?]): Query[A, Category] =
    val ref = Ref[A, NonScalarExpr, NonRestrictedConstructors]()
    Query.Filter[A, Category](this, Fun(ref, p(ref)))

  def filter(p: Ref[A, NonScalarExpr, NonRestrictedConstructors] => Expr[Boolean, NonScalarExpr, ?]): Query[A, Category] =
    withFilter(p)

  def nonEmpty: Expr[Boolean, NonScalarExpr, NonRestrictedConstructors] =
    Expr.NonEmpty(this)

  def isEmpty: Expr[Boolean, NonScalarExpr, NonRestrictedConstructors] =
    Expr.IsEmpty(this)

  // POC; the full CRUD API (Insert, Update, Delete) and each backend's SQL
  // syntax live on the `backend-specialization` branch.
  inline def insertInto[R]
    (table: InsertableTable[R, ?])
    (using @scala.annotation.implicitNotFound(
        "This dialect does not support INSERT ... SELECT. " +
        "Import a dialect that does, e.g. `import tyql.dialects.postgresql.given`.")
      insertable: dialects.DialectFeature.Insertable)
    (using @scala.annotation.implicitNotFound(
        "Types being inserted ${A} do not fit inside target types ${R}.")
      ev: TypeOperations.IsAcceptableInsertion[
        Tuple.Map[NamedTuple.DropNames[NamedTuple.From[A]], Expr.StripExpr],
        NamedTuple.DropNames[NamedTuple.From[R]]
      ])
  : InsertFromSelect[R, A] =
    val targetColumns =
      scala.compiletime.constValueTuple[NamedTuple.Names[NamedTuple.From[R]]]
        .toList.asInstanceOf[List[String]]
    InsertFromSelect(table.underlyingTable, this, targetColumns)

object Query:
  import Expr.{Fun, Ref}
  import RestrictedQuery.*

  /** Adapts an unrestricted function (QueryRef => Query) to work with fixImpl (RestrictedQueryRef => RestrictedQuery). */
  private def adaptUnrestricted[P, R](f: P => R): Any => Any = (refs: Any) =>
    val queryRefs = Tuple.fromArray(refs.asInstanceOf[Product].productIterator.map(
      _.asInstanceOf[RestrictedQueryRef[?, ?, ?, ?, ?]].toQueryRef
    ).toArray).asInstanceOf[P]
    val results = f(queryRefs)
    Tuple.fromArray(results.asInstanceOf[Product].productIterator.map { q =>
      val query = q.asInstanceOf[Query[?, ?]]
      RestrictedQuery(using query.tag)(query)
    }.toArray)

  def unrestrictedBagFix[QT <: Tuple](bases: QT)(using Tuple.Union[QT] <:< Query[?, ?])(fns: ToUnrestrictedQueryRef[QT] => ToUnrestrictedQuery[QT]): ToQuery[QT] =
    fixImpl(false)(bases)(adaptUnrestricted(fns).asInstanceOf[Nothing => Tuple])

  def unrestrictedFix[QT <: Tuple](bases: QT)(using Tuple.Union[QT] <:< Query[?, ?])(fns: ToUnrestrictedQueryRef[QT] => ToUnrestrictedQuery[QT]): ToQuery[QT] =
    fixImpl(true)(bases)(adaptUnrestricted(fns).asInstanceOf[Nothing => Tuple])


  type ExtractD[T <: Tuple] = InverseMapDeps[T]
  type Qref[QT <: Tuple] = ToRestrictedQueryRef[QT, RestrictedConstructors, MonotoneRestriction]

  /** Type class that bundles all linearity-dependent constraints for fix.
   *  Resolved at the call site where RL is concrete (Linear or NonLinear),
   *  then forwarded through instance methods without needing inline or match types.
   */
  /** Match type: when NoMutual, enforces single base case; when AllowMutual, always satisfied. */
  type CheckMutual[MR <: MutualRestriction, Qbase <: Tuple] = MR match
    case NoMutual => Tuple.Size[Qbase] =:= 1
    case AllowMutual => DummyImplicit

  /** Match type: when Linear, checks all recursive references appear at least once; when NonLinear, always satisfied. */
  type CheckLinearRelevance[RL <: LinearRestriction, QT <: Tuple, RQT <: Tuple] = RL match
    case Linear => IndexSequence[QT] =:= UnionDT[RQT]
    case NonLinear => DummyImplicit

  /** Match type: when Linear, checks no duplicate references; when NonLinear, always satisfied. */
  type CheckLinearDuplicates[RL <: LinearRestriction, RQT <: Tuple] = RL match
    case Linear => NoDuplicateReferences[RQT]
    case NonLinear => DummyImplicit

  // Formal rule: (1_κ, ..., i_κ) ≡ ∪ DT_i
  type IndexSequence[QT <: Tuple] = Tuple.Union[GenerateIndices[0, Tuple.Size[QT]]]
  type UnionDT[RQT <: Tuple] = Tuple.Union[Tuple.FlatMap[RQT, ExtractDependencies]]

  // κ-tagged variants: dep indices are `i & K` instead of bare `i`.
  type ToKRef[QT <: Tuple, K, RCF <: ConstructorFreedom, RM <: MonotoneRestriction] =
    Tuple.Map[Utils.ZipWithIndex[RestrictedQuery.Elems[QT]], [T] =>> T match
      case (elem, idx) => RestrictedQueryRef[elem, SetResult, idx & K, RCF, RM]]
  type IndexSequenceK[QT <: Tuple, K] =
    Tuple.Union[Tuple.Map[Utils.GenerateIndices[0, Tuple.Size[QT]], [I] =>> I & K]]
  type CheckLinearRelevanceK[RL <: LinearRestriction, QT <: Tuple, K, RQT <: Tuple] = RL match
    case Linear => IndexSequenceK[QT, K] =:= UnionDT[RQT]
    case NonLinear => DummyImplicit

  // Formal rule: ∀ DT_i |DT_i| ≡ |∪ DT_i| (checks no duplicates across all DT_i)
  // This is checked by ensuring the flattened tuple of all dependencies has no duplicates
  type AllDependencies[RQT <: Tuple] = Tuple.FlatMap[RQT, ExtractDependencies]
  type NoDuplicates[RQT <: Tuple] <: Boolean = HasDuplicate[AllDependencies[RQT]] match {
    case Nothing => false
    case _ => true
  }
  type NoDuplicateReferences[RQT <: Tuple] = NoDuplicates[RQT] =:= true

  // Extract the row type from Query union type
  type ExtractRowType[Q] = Q match {
    case Query[row, ?] => row
  }

  // Extract all row types from a tuple of queries
  type ExtractAllRowTypes[QT <: Tuple] <: Tuple = QT match {
    case EmptyTuple => EmptyTuple
    case Query[row, ?] *: tail => row *: ExtractAllRowTypes[tail]
  }

  // Check that a union of types is a subtype of AnyNamedTuple (which includes named tuples)
  // This constraint will verify all row types are Tuples (including named tuples)
  type AllRowTypesAreNamedTuples[QT <: Tuple] = Tuple.Union[ExtractAllRowTypes[QT]] <:< AnyNamedTuple
  type IsTupleOfQueries[QT <: Tuple] = Tuple.Union[QT] <:< Query[?, ?]
  type NoReferencesMissing[QT <: Tuple, RQT <: Tuple] = IndexSequence[QT] =:= UnionDT[RQT]

  type ToRQuery[Qbase <: Tuple, D <: Tuple] = ToRestrictedQuery[Qbase, D, RestrictedConstructors, MonotoneRestriction, SetResult]

  /** Companion `restrictedFix` with κ.  K is a method-level type param.
   *  Mirrors `FixWithK.restrictedFix`. */
  def restrictedFix[K, Qbase <: Tuple, Qret <: Tuple]
  (q: Qbase)
  (f: ToKRef[Qbase, K, RestrictedConstructors, MonotoneRestriction] => Qret)
  (using @implicitNotFound("Base cases must be of type Query: ${Qbase}") ev1:
  IsTupleOfQueries[Qbase])
  (using @implicitNotFound("Row types must be Tuples: ${Qbase}") evRowTypes:
  AllRowTypesAreNamedTuples[Qbase])
  (using @implicitNotFound("Number of base cases must match the number of recursive definitions returned by fns") ev0:
  Tuple.Size[Qbase] =:= Tuple.Size[Qret])
  (using @implicitNotFound("restrictedFix does not support mutual recursion, use fix to allow it") evMutual:
  Tuple.Size[Qbase] =:= 1)
  (using @implicitNotFound("Failed to generate recursive queries: ${Qret}") ev3:
  Qret <:< ToRQuery[Qbase, ExtractD[Qret]])
  (using @implicitNotFound("Recursive definitions must be linear with matching κ: ${Qret}") evK:
  IndexSequenceK[Qbase, K] =:= UnionDT[Qret])
  (using @implicitNotFound("Recursive definitions must be linear, e.g. recursive references cannot appear twice within the same recursive definition") ev5:
  NoDuplicateReferences[Qret])
  : ToQuery[Qbase] =
    fixImpl(true)(q)(f)

  /** Companion `fix` with κ.  K is a method-level type param.
   *  Mirrors `FixWithK.fix`. */
  def fix[
    K,
    Qbase <: Tuple, Qret <: Tuple,
    RCF <: ConstructorFreedom, RM <: MonotoneRestriction, RC <: ResultCategory, RL <: LinearRestriction, MR <: MutualRestriction
  ]
    (options: (constructorFreedom: RCF, monotonicity: RM, category: RC, linearity: RL, mutual: MR))
    (q: Qbase)
    (f: ToKRef[Qbase, K, RCF, RM] => Qret)
    (using @implicitNotFound("Base cases must be of type Query: ${Qbase}") ev1:
    IsTupleOfQueries[Qbase])
    (using @implicitNotFound("Row types must be Tuples: ${Qbase}") evRowTypes:
    AllRowTypesAreNamedTuples[Qbase])
    (using @implicitNotFound("Number of base cases must match the number of recursive definitions returned by fns") ev0:
    Tuple.Size[Qbase] =:= Tuple.Size[Qret])
    (using @implicitNotFound("Mutual recursion is not allowed with the current options") evMutual:
    CheckMutual[MR, Qbase])
    (using @implicitNotFound("Failed to generate recursive queries: ${Qret}") ev3:
    Qret <:< ToRestrictedQuery[Qbase, ToDependencyTuple[RL, Qret], RCF, RM, RC])
    (using @implicitNotFound("Recursive definitions must be linear with matching κ: ${Qret}") evK:
    CheckLinearRelevanceK[RL, Qbase, K, Qret])
    (using @implicitNotFound("Recursive definitions must be linear, e.g. recursive references cannot appear twice within the same recursive definition") ev5:
    CheckLinearDuplicates[RL, Qret])
  : ToQuery[Qbase] =
    fixImpl(options.category.isInstanceOf[SetResult])(q)(f)

  def fixImpl[QT <: Tuple, P <: Tuple, R <: Tuple, RCF <: ConstructorFreedom, RM <: MonotoneRestriction](setBased: Boolean, materialized: Boolean = false)(bases: QT)(fns: P => R): ToQuery[QT] =
    // If base cases are themselves recursive definitions.
    val baseRefsAndDefs = bases.toArray.map {
      case MultiRecursive(params, querys, resultQ, _) => ???// TODO: decide on semantics for multiple fix definitions. (param, query)
      case base: Query[?, ?] => (RestrictedQueryRef()(using base.tag), base)
    }
    val refsTuple = Tuple.fromArray(baseRefsAndDefs.map(_._1)).asInstanceOf[P]
    val refsList = baseRefsAndDefs.map(_._1).toList
    val recurQueries = fns(refsTuple)

    val baseCaseDefsList = baseRefsAndDefs.map(_._2.asInstanceOf[Query[?, ?]])
    val recursiveDefsList: List[Query[?, ?]] =
      if (setBased)
        recurQueries.toList.map(_.asInstanceOf[RestrictedQuery[?, ?, ?, RCF, RM]].toQuery).lazyZip(baseCaseDefsList).map:
          case (query: Query[t, c], ddef) =>
            // Optimization: remove any extra .distinct calls that are getting fed into a union anyway
            val lhs = ddef match
              case Distinct(from) => from
              case t => t
            val rhs = query match
              case Distinct(from) => from
              case t => t
            Union(lhs.asInstanceOf[Query[t, c]], rhs.asInstanceOf[Query[t, c]])(using query.tag)
      else
        recurQueries.toList.map(_.asInstanceOf[RestrictedQuery[?, ?, ?, RCF, RM]].toQuery).lazyZip(baseCaseDefsList).map:
          case (query: Query[t, c], ddef) =>
            UnionAll(ddef.asInstanceOf[Query[t, c]], query)(using query.tag)


    val rt = refsTuple.asInstanceOf[Tuple.Map[Elems[QT], [T] =>> RestrictedQueryRef[T, ?, ?, RCF, RM]]]
    rt.naturalMap([t] => finalRef =>
      val fr = finalRef.asInstanceOf[RestrictedQueryRef[t, ?, ?, RCF, RM]]
      given ResultTag[t] = fr.tag
      MultiRecursive(
        refsList,
        recursiveDefsList,
        fr.toQueryRef,
        materialized
      )
    )

  // TODO: in the case we want to allow bag semantics within recursive queries, set $bag
  case class MultiRecursive[R]($param: List[RestrictedQueryRef[?, ?, ?, ?, ?]],
                               $subquery: List[Query[?, ?]],
                               $resultQuery: Query[R, ?],
                               $materialized: Boolean = false)(using ResultTag[R]) extends Query[R, BagResult]

  private var refCount = 0
  case class QueryRef[A: ResultTag, C <: ResultCategory]() extends Query[A, C]:
    private val id = refCount
    refCount += 1
    def stringRef() = s"recref$id"
    override def toString: String = s"QueryRef[${stringRef()}]"

  case class QueryFun[A, B]($param: QueryRef[A, ?], $body: B)

  case class Filter[A: ResultTag, C <: ResultCategory]($from: Query[A, C], $pred: Fun[A, Expr[Boolean, ?, ?], ?, ?]) extends Query[A, C]
  case class Map[A, B: ResultTag]($from: Query[A, ?], $query: Fun[A, Expr[B, ?, ?], ?, ?]) extends Query[B, BagResult]
  case class FlatMap[A, B: ResultTag]($from: Query[A, ?], $query: Fun[A, Query[B, ?], NonScalarExpr, ?]) extends Query[B, BagResult]
  // case class Sort[A]($q: Query[A], $o: Ordering[A]) extends Query[A] // alternative syntax to avoid chaining .sort for multi-key sort
  case class Sort[A: ResultTag, B, C <: ResultCategory]($from: Query[A, C], $body: Fun[A, Expr[B, NonScalarExpr, ?], NonScalarExpr, ?], $ord: Ord) extends Query[A, C]
  case class Limit[A: ResultTag, C <: ResultCategory]($from: Query[A, C], $limit: Int) extends Query[A, C]
  case class Offset[A: ResultTag, C <: ResultCategory]($from: Query[A, C], $offset: Int) extends Query[A, C]
  case class Drop[A: ResultTag, C <: ResultCategory]($from: Query[A, C], $offset: Int) extends Query[A, C]
  case class Distinct[A: ResultTag]($from: Query[A, ?]) extends Query[A, SetResult]

  case class Union[A: ResultTag]($this: Query[A, ?], $other: Query[A, ?]) extends Query[A, SetResult]
  case class UnionAll[A: ResultTag]($this: Query[A, ?], $other: Query[A, ?]) extends Query[A, BagResult]
  case class Intersect[A: ResultTag]($this: Query[A, ?], $other: Query[A, ?]) extends Query[A, SetResult]
  case class IntersectAll[A: ResultTag]($this: Query[A, ?], $other: Query[A, ?]) extends Query[A, BagResult]
  case class Except[A: ResultTag]($this: Query[A, ?], $other: Query[A, ?]) extends Query[A, SetResult]
  case class ExceptAll[A: ResultTag]($this: Query[A, ?], $other: Query[A, ?]) extends Query[A, BagResult]

  case class NewGroupBy[
    AllSourceTypes <: Tuple,
    ResultType: ResultTag,
    GroupingType,
    GroupingShape <: ExprShape
  ]($source: Aggregation[AllSourceTypes, ResultType],
    $grouping: Expr[GroupingType, GroupingShape, ?],
    $sourceRefs: Seq[Ref[?, ?, ?]],
    $sourceTags: collection.Seq[(String, ResultTag[?])],
    $having: Option[Expr[Boolean, ?, ?]]) extends Query[ResultType, BagResult]:
    /**
     * Don't overload filter because having operates on the pre-grouped type.
     */
    def having(havingFn: ToNonScalarRef[AllSourceTypes] => Expr[Boolean, ?, ?]): Query[ResultType, BagResult] =
      if ($having.isEmpty)
        val refsTuple = Tuple.fromArray($sourceRefs.toArray).asInstanceOf[ToNonScalarRef[AllSourceTypes]]

        val havingResult = havingFn(refsTuple)
        NewGroupBy($source, $grouping, $sourceRefs, $sourceTags, Some(havingResult))
      else
        throw new Exception("Error: can only support a single having statement after groupBy")

  // NOTE: GroupBy is technically an aggregation but will return an interator of at least 1, like a query
  case class GroupBy[
    SourceType,
    ResultType: ResultTag,
    GroupingType,
    GroupingShape <: ExprShape,
    SelectShape <: ExprShape
  ]($source: Query[SourceType, ?],
    $groupingFn: Fun[SourceType, Expr[GroupingType, GroupingShape, NonRestrictedConstructors], GroupingShape, NonRestrictedConstructors],
    $selectFn: Fun[SourceType, Expr[ResultType, SelectShape, NonRestrictedConstructors], SelectShape, NonRestrictedConstructors],
    $havingFn: Option[Fun[SourceType, Expr[Boolean, ?, NonRestrictedConstructors], ?, NonRestrictedConstructors]]) extends Query[ResultType, BagResult]:
    /**
     * Don't overload filter because having operates on the pre-grouped type.
     */
    def having(p: Ref[SourceType, ?, NonRestrictedConstructors] => Expr[Boolean, ?, NonRestrictedConstructors]): Query[ResultType, BagResult] =
      if ($havingFn.isEmpty)
        val ref = Ref[SourceType, NonScalarExpr, NonRestrictedConstructors]()(using $source.tag)
        val fun = Fun(ref, p(ref))
        GroupBy($source, $groupingFn, $selectFn, Some(fun))
      else
        throw new Exception("Error: can only support a single having statement after groupBy")

  // Extensions. TODO: Any reason not to move these into Query methods?
  extension [R: ResultTag, C <: ResultCategory](x: Query[R, C])
    /**
     * When there is only one relation to be defined recursively.
     */
    def sort[B](f: Ref[R, NonScalarExpr, NonRestrictedConstructors] => Expr[B, NonScalarExpr, NonRestrictedConstructors], ord: Ord): Query[R, C] =
      val ref = Ref[R, NonScalarExpr, NonRestrictedConstructors]()
      Sort(x, Fun(ref, f(ref)), ord)

    def limit(lim: Int): Query[R, C] = Limit(x, lim)
    def take(lim: Int): Query[R, C] = limit(lim)

    def offset(lim: Int): Query[R, C] = Offset(x, lim)
    def drop(lim: Int): Query[R, C] = offset(lim)

    def distinct: Query[R, SetResult] = Distinct(x)

    def sum[B: ResultTag](f: Ref[R, NonScalarExpr, NonRestrictedConstructors] => Expr[B, NonScalarExpr, NonRestrictedConstructors]): Aggregation[R *: EmptyTuple, B] =
      val ref = Ref[R, NonScalarExpr, NonRestrictedConstructors]()
       Aggregation.AggFlatMap(x, Fun(ref, AggregationExpr.Sum(f(ref))))

    def avg[B: ResultTag](f: Ref[R, NonScalarExpr, NonRestrictedConstructors] => Expr[B, NonScalarExpr, NonRestrictedConstructors]): Aggregation[R *: EmptyTuple, B] =
      val ref = Ref[R, NonScalarExpr, NonRestrictedConstructors]()
       Aggregation.AggFlatMap(x, Fun(ref, AggregationExpr.Avg(f(ref))))

    def max[B: ResultTag](f: Ref[R, NonScalarExpr, NonRestrictedConstructors] => Expr[B, NonScalarExpr, NonRestrictedConstructors]): Aggregation[R *: EmptyTuple, B] =
      val ref = Ref[R, NonScalarExpr, NonRestrictedConstructors]()
       Aggregation.AggFlatMap(x, Fun(ref, AggregationExpr.Max(f(ref))))

    def min[B: ResultTag](f: Ref[R, NonScalarExpr, NonRestrictedConstructors] => Expr[B, NonScalarExpr, NonRestrictedConstructors]): Aggregation[R *: EmptyTuple, B] =
      val ref = Ref[R, NonScalarExpr, NonRestrictedConstructors]()
       Aggregation.AggFlatMap(x, Fun(ref, AggregationExpr.Min(f(ref))))

    def size: Aggregation[R *: EmptyTuple, Int] =
      val ref = Ref[R, ScalarExpr, NonRestrictedConstructors]()
      Aggregation.AggFlatMap(x, Fun(ref, AggregationExpr.Count(Expr.IntLit(1))))

    def union(that: Query[R, ?]): Query[R, SetResult] =
      Union(x, that)

    def unionAll(that: Query[R, ?]): Query[R, BagResult] =
      UnionAll(x, that)

    def intersect(that: Query[R, ?]): Query[R, SetResult] =
      Intersect(x, that)

    def intersectAll(that: Query[R, ?]): Query[R, BagResult] =
      IntersectAll(x, that)

    def except(that: Query[R, ?]): Query[R, SetResult] =
      Except(x, that)

    def exceptAll(that: Query[R, ?]): Query[R, BagResult] =
      ExceptAll(x, that)

    // Does not work for subsets, need to match types exactly
    def contains(that: Expr[R, NonScalarExpr, NonRestrictedConstructors]): Expr[Boolean, NonScalarExpr, NonRestrictedConstructors] =
      Expr.Contains(x, that)


  // def single(): R =
    //   Expr.Single(x)

end Query

/* The following is not needed currently

/** A type class for types that can map to a database table */
trait Row:
  type Self
  type Fields = NamedTuple.From[Self]
  type FieldExprs = NamedTuple.Map[Fields, Expr]

  //def toFields(x: Self): Fields = ???
  //def fromFields(x: Fields): Self = ???

*/
