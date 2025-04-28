package tyql

import language.experimental.namedTuples
import NamedTuple.{AnyNamedTuple, NamedTuple}
import scala.annotation.{implicitNotFound, targetName}
import scala.reflect.ClassTag
import Utils.{HasDuplicate, naturalMap}
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

  def customFix[RCF <: ConstructorFreedom, RM <: MonotoneRestriction, RC <: ResultCategory, RL <: LinearRestriction, DT1 <: Tuple]
    (options: (constructorFreedom: RCF, monotonicity: RM, category: RC, linearity: RL))
    (using @implicitNotFound("customFix nonlinear constraint") ev0: LinearConstraint[RL, DT1]) // POC combine type constraints
    (p: RestrictedQueryRef[A, ?, 0, RCF, RM] => RestrictedQuery[A, RC, DT1, RCF, RM])
  : Query[A, RC] =
    // override wtih NonLinear because linearity constraint already checked by this method
    val nextOptions = (
      constructorFreedom = options.constructorFreedom,
      monotonicity = options.monotonicity,
      category = options.category,
      linearity = NonLinear()
    )
    type QT = Tuple1[Query[A, ?]]
    val result = options.linearity match
      case _: Linear =>
        val nextP = p.asInstanceOf[RestrictedQueryRef[A, ?, 0, RCF, RM] => RestrictedQuery[A, RC, Tuple1[0], RCF, RM]]
        type RQT = Tuple1[RestrictedQuery[A, RC, Tuple1[0], RCF, RM]]
        val fn: Tuple1[RestrictedQueryRef[A, ?, 0, RCF, RM]] => RQT = r => Tuple1(nextP(r._1))
        Query.customFix[QT, Tuple1[Tuple1[0]], RQT, RCF, RM, RC, NonLinear](nextOptions)(Tuple1(this))(fn)._1
      case _: NonLinear =>
        type RQT = Tuple1[RestrictedQuery[A, RC, DT1, RCF, RM]]
        val fn: Tuple1[RestrictedQueryRef[A, ?, 0, RCF, RM]] => RQT = r => Tuple1(p(r._1))
        Query.customFix[QT, Tuple1[DT1], RQT, RCF, RM, RC, NonLinear](nextOptions)(Tuple1(this))(fn)._1
    result.asInstanceOf[Query[A, RC]]

  def fix(p: RestrictedQueryRef[A, ?, 0, RestrictedConstructors, MonotoneRestriction] => RestrictedQuery[A, SetResult, Tuple1[0], RestrictedConstructors, MonotoneRestriction]): Query[A, SetResult] =
    type QT = Tuple1[Query[A, ?]]
    type DT = Tuple1[Tuple1[0]]
    type RQT = Tuple1[RestrictedQuery[A, SetResult, Tuple1[0], RestrictedConstructors, MonotoneRestriction]]
    val fn: Tuple1[RestrictedQueryRef[A, ?, 0, RestrictedConstructors, MonotoneRestriction]] => RQT = r => Tuple1(p(r._1))
    Query.fix[QT, DT, RQT](Tuple1(this))(fn)._1

  def fixConstructorFree(p: RestrictedQueryRef[A, ?, 0, NonRestrictedConstructors, MonotoneRestriction] => RestrictedQuery[A, SetResult, Tuple1[0], NonRestrictedConstructors, MonotoneRestriction]): Query[A, SetResult] =
    type QT = Tuple1[Query[A, ?]]
    type DT = Tuple1[Tuple1[0]]
    type RQT = Tuple1[RestrictedQuery[A, SetResult, Tuple1[0], NonRestrictedConstructors, MonotoneRestriction]]
    val fn: Tuple1[RestrictedQueryRef[A, ?, 0, NonRestrictedConstructors, MonotoneRestriction]] => RQT = r => Tuple1(p(r._1))
    Query.fixConstructorFree[QT, DT, RQT](Tuple1(this))(fn)._1

  def unrestrictedFix(p: Query.QueryRef[A, ?] => Query[A, ?]): Query[A, BagResult] =
    type QT = Tuple1[Query[A, ?]]
    val fn: Tuple1[Query.QueryRef[A, ?]] => Tuple1[Query[A, ?]] = r => Tuple1(p(r._1))
    val t = Query.unrestrictedFix[QT](Tuple1(this))(fn)._1
    t.asInstanceOf[Query[A, BagResult]]

  def unrestrictedBagFix(p: Query.QueryRef[A, ?] => Query[A, ?]): Query[A, BagResult] =
    type QT = Tuple1[Query[A, ?]]
    val fn: Tuple1[Query.QueryRef[A, ?]] => Tuple1[Query[A, ?]] = r => Tuple1(p(r._1))
    val t = Query.unrestrictedBagFix[QT](Tuple1(this))(fn)._1
    t.asInstanceOf[Query[A, BagResult]]

  def withFilter(p: Ref[A, NonScalarExpr, NonRestrictedConstructors] => Expr[Boolean, NonScalarExpr, ?]): Query[A, Category] =
    val ref = Ref[A, NonScalarExpr, NonRestrictedConstructors]()
    Query.Filter[A, Category](this, Fun(ref, p(ref)))

  def filter(p: Ref[A, NonScalarExpr, NonRestrictedConstructors] => Expr[Boolean, NonScalarExpr, ?]): Query[A, Category] =
    withFilter(p)

  def nonEmpty: Expr[Boolean, NonScalarExpr, NonRestrictedConstructors] =
    Expr.NonEmpty(this)

  def isEmpty: Expr[Boolean, NonScalarExpr, NonRestrictedConstructors] =
    Expr.IsEmpty(this)

object Query:
  import Expr.{Fun, Ref}
  import RestrictedQuery.*

  def monotoneFix[QT <: Tuple](bases: QT)(using Tuple.Union[QT] <:< Query[?, ?])(fns: ToMonotoneQueryRef[QT] => ToMonotoneQuery[QT]): ToQuery[QT] =
    fixImpl(false)(bases)(fns)

  // No restrictions at all but still use set semantics (union between base + recur)
  def unrestrictedFix[QT <: Tuple](bases: QT)(using Tuple.Union[QT] <:< Query[?, ?])(fns: ToUnrestrictedQueryRef[QT] => ToUnrestrictedQuery[QT]): ToQuery[QT] =
    unrestrictedFixImpl(true)(bases)(fns)

  // No restrictions at all, use bag semantics
  def unrestrictedBagFix[QT <: Tuple](bases: QT)(using Tuple.Union[QT] <:< Query[?, ?])(fns: ToUnrestrictedQueryRef[QT] => ToUnrestrictedQuery[QT]): ToQuery[QT] =
    unrestrictedFixImpl(false)(bases)(fns)

  def fixConstructorFree[QT <: Tuple, DT <: Tuple, RQT <: Tuple]
  (bases: QT)
  (fns: ToRestrictedQueryRef[QT, NonRestrictedConstructors, MonotoneRestriction] => RQT)
  (using @implicitNotFound("Number of base cases must match the number of recursive definitions returned by fns") ev0: Tuple.Size[QT] =:= Tuple.Size[RQT])
  (using @implicitNotFound("Base cases must be of type Query: ${QT}") ev1: Union[QT] <:< Query[?, ?])
  (using @implicitNotFound("Cannot extract dependencies, is the query affine?") ev2: DT <:< InverseMapDeps[RQT])
  (using @implicitNotFound("Failed to generate recursive queries: ${RQT}") ev3: RQT <:< ToRestrictedQuery[QT, DT, NonRestrictedConstructors, MonotoneRestriction, SetResult])
  (using @implicitNotFound("Recursive definitions must be linear, e.g. recursive references must appear at least once in all the recursive definitions: ${RQT}") ev4: ExpectedResult[QT] <:< ActualResult[RQT])
  : ToQuery[QT] =
    fixImpl(true)(bases)(fns)

  /**
   * Fixed point computation.
   *
   * @param bases Tuple of the base case queries. NOTE: bases do not have to be sets since they will be directly fed into an union.
   * TODO: in theory, the results could be assumed to be sets due to the outermost UNION(base, recur), but better to enforce .distinct (also bc we flatten the unions)
   * @param fns A function from a tuple of restricted query refs to a tuple of *SET* restricted queries.
   *
   * QT is the tuple of Query
   * DT is the tuple of tuple of constant ints. Used to track dependencies of recursive definitions in order to enforce that every recursive reference is used at least once.
   * RQT is the tuple of RestrictedQuery that should be returned from fns.
   *
   * NOTE: Because groupBy is a Query not an Aggregation, the results of groupBy are allowed as base-cases of recursive queries.
   * This is consistent with stratified aggregation. However, if you wanted to prevent recursion of any kind within mutual recursive
   * queries (e.g. postgres), would need to add a constraint to prevent fix(...) from taking in anything of type GroupBy within QT.
   * Not applicable to inline fix, since the restriction is only for mutual recursion.
   */
  def fix[QT <: Tuple, DT <: Tuple, RQT <: Tuple]
                                    (bases: QT)
                                    (fns: ToRestrictedQueryRef[QT, RestrictedConstructors, MonotoneRestriction] => RQT)
                                    (using @implicitNotFound("Number of base cases must match the number of recursive definitions returned by fns") ev0: Tuple.Size[QT] =:= Tuple.Size[RQT])
                                    (using @implicitNotFound("Base cases must be of type Query: ${QT}") ev1: Union[QT] <:< Query[?, ?])
                                    (using @implicitNotFound("Cannot extract dependencies, is the query affine?") ev2: DT <:< InverseMapDeps[RQT])
                                    (using @implicitNotFound("Failed to generate recursive queries: ${RQT}") ev3: RQT <:< ToRestrictedQuery[QT, DT, RestrictedConstructors, MonotoneRestriction, SetResult])
                                    (using @implicitNotFound("Recursive definitions must be linear, e.g. recursive references must appear at least once in all the recursive definitions: ${RQT}") ev4: ExpectedResult[QT] <:< ActualResult[RQT])
//                                    (using @implicitNotFound("Recursive definitions must be linear, e.g. recursive references cannot appear twice within the same recursive definition: ${RQT}") ev2: Tuple.Union[Tuple.Map[DT, CheckDuplicate]] =:= false)
      : ToQuery[QT] =
    fixImpl(true)(bases)(fns)

  /* Specify which constraints you want to apply with options parameters */
  def customFix[QT <: Tuple, DT <: Tuple, RQT <: Tuple, RCF <: ConstructorFreedom, RM <: MonotoneRestriction, RC <: ResultCategory, RL <: LinearRestriction]
    (options: (constructorFreedom: RCF, monotonicity: RM, category: RC, linearity: RL))
    (bases: QT)
    (fns: ToRestrictedQueryRef[QT, RCF, RM] => RQT)
  (using @implicitNotFound("Number of base cases must match the number of recursive definitions returned by fns") ev0: Tuple.Size[QT] =:= Tuple.Size[RQT])
  (using @implicitNotFound("Base cases must be of type Query: ${QT}") ev1: Union[QT] <:< Query[?, ?])
  (using @implicitNotFound("Cannot extract dependencies, is the query affine?: ${RQT}") ev2: DT <:< ToDependencyTuple[RL, RQT])
  (using @implicitNotFound("Failed to generate recursive queries: ${RQT}") ev3: RQT <:< ToRestrictedQuery[QT, DT, RCF, RM, RC])
  (using @implicitNotFound("Recursive definitions must be linear, e.g. recursive references must appear at least once in all the recursive definitions: ${RL}") ev4: CheckRelevantQuery[RL, QT, RQT] <:< true)
  : ToQuery[QT] =
    fixImpl(options.category.isInstanceOf[SetResult])(bases)(fns)

  def unrestrictedFixImpl[QT <: Tuple, P <: Tuple, R <: Tuple](setBased: Boolean)(bases: QT)(fns: P => R): ToQuery[QT] =
    // If base cases are themselves recursive definitions.
    val baseRefsAndDefs = bases.toArray.map {
      case MultiRecursive(params, querys, resultQ) => ??? // TODO: decide on semantics for multiple fix definitions. (param, query)
      case base: Query[?, ?] => (RestrictedQueryRef()(using base.tag), base)
    }
    val refsTuple = Tuple.fromArray(baseRefsAndDefs.map(_._1)).asInstanceOf[P]
    val unrestrictedRefsTuple = Tuple.fromArray(baseRefsAndDefs.map(_._1.toQueryRef)).asInstanceOf[P]
    val refsList = baseRefsAndDefs.map(_._1).toList
    val recurQueries = fns(unrestrictedRefsTuple)

    val baseCaseDefsList = baseRefsAndDefs.map(_._2.asInstanceOf[Query[?, ?]])
    val recursiveDefsList: List[Query[?, ?]] =
      if (setBased)
        recurQueries.toList.map(_.asInstanceOf[Query[?, ?]]).lazyZip(baseCaseDefsList).map:
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
        recurQueries.toList.map(_.asInstanceOf[Query[?, ?]]).lazyZip(baseCaseDefsList).map:
          case (query: Query[t, c], ddef) =>
            UnionAll(ddef.asInstanceOf[Query[t, c]], query.asInstanceOf[Query[t, c]])(using query.tag)

    val rt = refsTuple.asInstanceOf[Tuple.Map[Elems[QT], [T] =>> RestrictedQueryRef[T, ?, ?, NonRestrictedConstructors, NonMonotone]]]
    rt.naturalMap([t] => finalRef =>
      val fr = finalRef.asInstanceOf[RestrictedQueryRef[t, ?, ?, NonRestrictedConstructors, NonMonotone]]

      given ResultTag[t] = fr.tag

      MultiRecursive(
        refsList,
        recursiveDefsList,
        fr.toQueryRef
      )
    )

  def fixImpl[QT <: Tuple, P <: Tuple, R <: Tuple, RCF <: ConstructorFreedom, RM <: MonotoneRestriction](setBased: Boolean)(bases: QT)(fns: P => R): ToQuery[QT] =
    // If base cases are themselves recursive definitions.
    val baseRefsAndDefs = bases.toArray.map {
      case MultiRecursive(params, querys, resultQ) => ???// TODO: decide on semantics for multiple fix definitions. (param, query)
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
            UnionAll(query, ddef.asInstanceOf[Query[t, c]])(using query.tag)


    val rt = refsTuple.asInstanceOf[Tuple.Map[Elems[QT], [T] =>> RestrictedQueryRef[T, ?, ?, RCF, RM]]]
    rt.naturalMap([t] => finalRef =>
      val fr = finalRef.asInstanceOf[RestrictedQueryRef[t, ?, ?, RCF, RM]]
      given ResultTag[t] = fr.tag
      MultiRecursive(
        refsList,
        recursiveDefsList,
        fr.toQueryRef
      )
    )

  // TODO: in the case we want to allow bag semantics within recursive queries, set $bag
  case class MultiRecursive[R]($param: List[RestrictedQueryRef[?, ?, ?, ?, ?]],
                               $subquery: List[Query[?, ?]],
                               $resultQuery: Query[R, ?])(using ResultTag[R]) extends Query[R, SetResult]

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
