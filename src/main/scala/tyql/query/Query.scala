package tyql

import language.experimental.namedTuples
import NamedTuple.{AnyNamedTuple, NamedTuple}
import scala.annotation.{implicitNotFound, targetName}
import scala.reflect.ClassTag
import Utils.{HasDuplicate, naturalMap}
import tyql.Expr.StripExpr
import tyql.RestrictedQuery.ToRestrictedQueryRef

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
  def flatMap[B: ResultTag](f: Ref[A, NonScalarExpr] => Query[B, ?]): Query[B, BagResult] =
    val ref = Ref[A, NonScalarExpr]()
    Query.FlatMap(this, Fun(ref, f(ref)))

  /**
   * Classic flatMap with an inner query that is a RestrictedQuery.
   * This turns the result query into a RestrictedQuery.
   * Exists to support doing BaseCaseRelation.flatMap(...) within a fix
   * @param f   a function that returns a RestrictedQuery, meaning it has used a recursive definition from fix.
   * @tparam B  the result type of the query.
   * @return    RestrictedQuery[B]
   */
  def flatMap[B: ResultTag, D <: Tuple](f: Ref[A, NonScalarExpr] => RestrictedQuery[B, ?, D]): RestrictedQuery[B, BagResult, D] =
    val ref = Ref[A, NonScalarExpr]()
    RestrictedQuery(Query.FlatMap(this, Fun(ref, f(ref).toQuery)))
  /**
   * Equivalent to aggregate(f: Ref => Aggregation).
   * NOTE: make Ref of type NExpr so that relation.id is counted as NExpr, not ScalarExpr
   *
   * @param f   a function that returns an Aggregation (guaranteed agg in subtree)
   * @tparam B  the result type of the aggregation.
   * @return    Aggregation[B], a scalar result, e.g. a single value of type B.
   */
  def aggregate[B: ResultTag](f: Ref[A, NonScalarExpr] => Aggregation[B]): Aggregation[B] =
    val ref = Ref[A, NonScalarExpr]()
    Aggregation.AggFlatMap(this, Fun(ref, f(ref)))

  /**
   * Equivalent version of map(f: Ref => Aggregation).
   * Requires f to call toRow on the final result before returning.
   * Sometimes the implicit conversion kicks in and converts a named-tuple-of-Agg into a Agg-of-named-tuple,
   * but not always. As an alternative, can use the aggregate defined below that explicitly calls toRow on the result of f.
   *
   * @param f   a function that returns an Aggregation (guaranteed agg in subtree)
   * @tparam B  the result type of the aggregation.
   * @return    Aggregation[B], a scalar result, e.g. single value of type B.
   */
  @targetName("AggregateExpr")
  def aggregate[B: ResultTag](f: Ref[A, NonScalarExpr] => AggregationExpr[B]): Aggregation[B] =
    val ref = Ref[A, NonScalarExpr]()
    Aggregation.AggFlatMap(this, Fun(ref, f(ref)))

  /**
   * A version of the above-defined aggregate that allows users to skip calling toRow on the result in f.
   *
   * @param f    a function that returns a named-tuple-of-Aggregation.
   * @tparam B   the named-tuple-of-Aggregation that will be converted to an Aggregation-of-named-tuple
   * @return     Aggregation of B.toRow, e.g. a scalar result of type B.toRow
   */
  def aggregate[B <: AnyNamedTuple: AggregationExpr.IsTupleOfAgg]/*(using ev: AggregationExpr.IsTupleOfAgg[B] =:= true)*/(using ResultTag[NamedTuple.Map[B, Expr.StripExpr]])(f: Ref[A, NonScalarExpr] => B): Aggregation[ NamedTuple.Map[B, Expr.StripExpr] ] =
    import AggregationExpr.toRow
    val ref = Ref[A, NonScalarExpr]()
    val row = f(ref).toRow
    Aggregation.AggFlatMap(this, Fun(ref, row))

//  inline def aggregate[B: ResultTag](f: Ref[A, ScalarExpr] => Query[B]): Nothing =
//    error("No aggregation function found in f. Did you mean to use flatMap?")

// TODO: Restrictions for groupBy: all columns in the selectFn must either be in the groupingFn or in an aggregate.
//  type GetFields[T] <: Tuple = T match
//    case Expr[t, ?] => GetFields[t]
//    case NamedTuple[n, v] => n
  /**
   * TODO: Right now this most closely resembles SQL groupBy, not Spark RDD's or pairs. Do we want to pick one?
   *
   * groupBy where the grouping clause is NOT an aggregation. Hopefully we can merge or overload groupBy and groupByAggregate
   * but for now keep separate.
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
    groupingFn: Ref[A, NonScalarExpr] => Expr[GroupResult, NonScalarExpr],
    selectFn: Ref[A, ScalarExpr] => Expr[R, ScalarExpr]
//  (using ev: Tuple.Union[GetFields[A]] <:< Tuple.Union[GetFields[G]])
   ): Query.GroupBy[A, R, GroupResult, NonScalarExpr, ScalarExpr] =
    val refG = Ref[A, NonScalarExpr]()
    val groupFun = Fun(refG, groupingFn(refG))

    val refS = Ref[A, ScalarExpr]()
    val selectFun = Fun(refS, selectFn(refS))
    Query.GroupBy(this, groupFun, selectFun, None)

  /**
   * groupBy where the grouping clause IS an aggregation. Hopefully we can merge or overload groupBy and groupByAggregate
   * but for now keep separate.
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
    groupingFn: Ref[A, ScalarExpr] => Expr[GroupResult, ScalarExpr],
    selectFn: Ref[A, ScalarExpr] => Expr[R, ScalarExpr]
  ): Query.GroupBy[A, R, GroupResult, ScalarExpr, ScalarExpr] =
    val refG = Ref[A, ScalarExpr]()
    val groupFun = Fun(refG, groupingFn(refG))

    val refS = Ref[A, ScalarExpr]()
    val selectFun = Fun(refS, selectFn(refS))
    Query.GroupBy(this, groupFun, selectFun, None)

  /**
   * filter based on a groupBy result. Separate for now, hopefully can merge with groupBy/groupByAggregate.
   * The selectFn MUST NOT return an aggregation.
   * The groupingFn MUST NOT return an aggregation
   *
   * @param groupingFn - must be a named tuple, in order from left->right. Must return a scalar expression.
   * @param selectFn - the project clause of the select statement that is grouped. Must return a scalar expression.
   * @param havingFn - the filter clause. Must return an aggregation.
   * @tparam R - the return type of the expression
   * @tparam GroupResult - the type of the grouping statement
   * @return
   */
  def filterByGroupBy[R: ResultTag, GroupResult](
                                          groupingFn: Ref[A, NonScalarExpr] => Expr[GroupResult, NonScalarExpr],
                                          selectFn: Ref[A, NonScalarExpr] => Expr[R, NonScalarExpr],
                                          havingFn: Ref[A, ?] => Expr[Boolean, ?]
                                        ): Query.GroupBy[A, R, GroupResult, NonScalarExpr, NonScalarExpr] =
    val refG = Ref[A, NonScalarExpr]()
    val groupFun = Fun(refG, groupingFn(refG))

    val refS = Ref[A, NonScalarExpr]()
    val selectFun = Fun(refS, selectFn(refS))

    // Workaround for: "ScalarExpr is not subtype of ?"
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
  def map[B: ResultTag](f: Ref[A, NonScalarExpr] => Expr[B, NonScalarExpr]): Query[B, BagResult] =
    val ref = Ref[A, NonScalarExpr]()
    Query.Map(this, Fun(ref, f(ref)))

/**
   * A version of the above-defined map that allows users to skip calling toRow on the result in f.
   *
   * @param f    a function that returns a named-tuple-of-Expr.
   * @tparam B   the named-tuple-of-Expr that will be converted to an Expr-of-named-tuple
   * @return     Expr of B.toRow, e.g. an iterable of type B.toRow
   */
  def map[B <: AnyNamedTuple : Expr.IsTupleOfExpr](using ResultTag[NamedTuple.Map[B, Expr.StripExpr]])(f: Ref[A, NonScalarExpr] => B): Query[ NamedTuple.Map[B, Expr.StripExpr], BagResult ] =
    import Expr.toRow
    val ref = Ref[A, NonScalarExpr]()
    Query.Map(this, Fun(ref, f(ref).toRow))

  def fix(p: RestrictedQueryRef[A, ?, 0] => RestrictedQuery[A, SetResult, Tuple1[0]]): Query[A, SetResult] =
    type QT = Tuple1[Query[A, ?]]
    type DT = Tuple1[Tuple1[0]]
    type RQT = Tuple1[RestrictedQuery[A, SetResult, Tuple1[0]]]
    val fn: Tuple1[RestrictedQueryRef[A, ?, 0]] => RQT = r => Tuple1(p(r._1))
//    Query.fix(Tuple1(this))(fn)._1
    Query.fix[QT, DT, RQT](Tuple1(this))(fn)._1
//    Query.fix[Tuple1[Tuple1[0]], Tuple1[Query[A, ?]]](Tuple1(this))(fn)._1

  def withFilter(p: Ref[A, NonScalarExpr] => Expr[Boolean, NonScalarExpr]): Query[A, Category] =
    val ref = Ref[A, NonScalarExpr]()
    Query.Filter(this, Fun(ref, p(ref)))

  def filter(p: Ref[A, NonScalarExpr] => Expr[Boolean, NonScalarExpr]): Query[A, Category] = withFilter(p)

object Query:
  import Expr.{Pred, Fun, Ref}
  import RestrictedQuery.*

  def unrestrictedFix[QT <: Tuple](bases: QT)(using Tuple.Union[QT] <:< Query[?, ?])(fns: ToMonotoneQueryRef[QT] => ToMonotoneQuery[QT]): ToQuery[QT] =
    fixImpl(bases)(fns)

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
   */
  def fix[QT <: Tuple, DT <: Tuple, RQT <: Tuple]
                                    (bases: QT)
                                    (fns: ToRestrictedQueryRef[QT] => RQT)
                                    (using @implicitNotFound("Number of base cases must match the number of recursive definitions returned by fns") ev0: Tuple.Size[QT] =:= Tuple.Size[RQT])
                                    (using @implicitNotFound("Base cases must be of type Query: ${QT}") ev1: Union[QT] <:< Query[?, ?])
                                    (using @implicitNotFound("Cannot constrain DT") ev2: DT <:< InverseMapDeps[RQT])
                                    (using @implicitNotFound("Recursive definitions must be linear: ${RQT}") ev3: RQT <:< ToRestrictedQuery[QT, DT])
                                    (using @implicitNotFound("Recursive definitions must be linear, e.g. recursive references must appear at least once in all the recursive definitions: ${RQT}") ev4: ExpectedResult[QT] <:< ActualResult[RQT])
//                                    (using @implicitNotFound("Recursive definitions must be linear, e.g. recursive references cannot appear twice within the same recursive definition: ${RQT}") ev2: Tuple.Union[Tuple.Map[DT, CheckDuplicate]] =:= false)
      : ToQuery[QT] =
    fixImpl(bases)(fns)

  def fixImpl[QT <: Tuple, P <: Tuple, R <: Tuple](bases: QT)(fns: P => R): ToQuery[QT] =
    // If base cases are themselves recursive definitions.
    val baseRefsAndDefs = bases.toArray.map {
      case MultiRecursive(params, querys, resultQ) => ???// TODO: decide on semantics for multiple fix definitions. (param, query)
      case base: Query[?, ?] => (RestrictedQueryRef()(using base.tag), base)
    }
    val refsTuple = Tuple.fromArray(baseRefsAndDefs.map(_._1)).asInstanceOf[P]
    val refsList = baseRefsAndDefs.map(_._1).toList
    val recurQueries = fns(refsTuple)

    val baseCaseDefsList = baseRefsAndDefs.map(_._2.asInstanceOf[Query[?, ?]])
    val recursiveDefsList: List[Query[?, ?]] = recurQueries.toList.map(_.asInstanceOf[RestrictedQuery[?, ?, ?]].toQuery).lazyZip(baseCaseDefsList).map:
      case (query: Query[t, c], ddef) =>
        // Optimization: remove any extra .distinct calls that are getting fed into a union anyway
        val lhs = ddef match
          case Distinct(from) => from
          case t => t
        val rhs = query match
          case Distinct(from) => from
          case t => t
        Union(lhs.asInstanceOf[Query[t, c]], rhs.asInstanceOf[Query[t, c]])(using query.tag)

    val rt = refsTuple.asInstanceOf[Tuple.Map[Elems[QT], [T] =>> RestrictedQueryRef[T, ?, ?]]]
    rt.naturalMap([t] => finalRef =>
      val fr = finalRef.asInstanceOf[RestrictedQueryRef[t, ?, ?]]
      given ResultTag[t] = fr.tag
      MultiRecursive(
        refsList,
        recursiveDefsList,
        fr.toQueryRef
      )
    )

  case class MultiRecursive[R]($param: List[RestrictedQueryRef[?, ?, ?]],
                               $subquery: List[Query[?, ?]],
                               $resultQuery: Query[R, ?])(using ResultTag[R]) extends Query[R, SetResult]

  private var refCount = 0
  case class QueryRef[A: ResultTag, C <: ResultCategory]() extends Query[A, C]:
    private val id = refCount
    refCount += 1
    def stringRef() = s"recref$id"
    override def toString: String = s"QueryRef[${stringRef()}]"

  case class QueryFun[A, B]($param: QueryRef[A, ?], $body: B)

  case class Filter[A: ResultTag, C <: ResultCategory]($from: Query[A, C], $pred: Pred[A, NonScalarExpr]) extends Query[A, C]
  case class Map[A, B: ResultTag]($from: Query[A, ?], $query: Fun[A, Expr[B, ?], ?]) extends Query[B, BagResult]
  case class FlatMap[A, B: ResultTag]($from: Query[A, ?], $query: Fun[A, Query[B, ?], NonScalarExpr]) extends Query[B, BagResult]
  // case class Sort[A]($q: Query[A], $o: Ordering[A]) extends Query[A] // alternative syntax to avoid chaining .sort for multi-key sort
  case class Sort[A: ResultTag, B, C <: ResultCategory]($from: Query[A, C], $body: Fun[A, Expr[B, NonScalarExpr], NonScalarExpr], $ord: Ord) extends Query[A, C]
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

  // NOTE: GroupBy is technically an aggregation but will return an interator of at least 1, like a query
  case class GroupBy[
    SourceType,
    ResultType: ResultTag,
    GroupingType,
    GroupingShape <: ExprShape,
    SelectShape <: ExprShape
  ]($source: Query[SourceType, ?],
    $groupingFn: Fun[SourceType, Expr[GroupingType, GroupingShape], GroupingShape],
    $selectFn: Fun[SourceType, Expr[ResultType, SelectShape], SelectShape],
    $havingFn: Option[Fun[SourceType, Expr[Boolean, ?], ?]]) extends Query[ResultType, BagResult]:
    /**
     * Don't overload filter because having operates on the pre-grouped type.
     */
    def having(p: Ref[SourceType, ?] => Expr[Boolean, ?]): Query[ResultType, BagResult] =
      if ($havingFn.isEmpty)
        val ref = Ref[SourceType, NonScalarExpr]()(using $source.tag)
        val fun = Fun(ref, p(ref))
        GroupBy($source, $groupingFn, $selectFn, Some(fun))
      else
        throw new Exception("Error: can only support a single having statement after groupBy")

  // Extensions. TODO: Any reason not to move these into Query methods?
  extension [R: ResultTag, C <: ResultCategory](x: Query[R, C])
    /**
     * When there is only one relation to be defined recursively.
     */
    def sort[B](f: Ref[R, NonScalarExpr] => Expr[B, NonScalarExpr], ord: Ord): Query[R, C] =
      val ref = Ref[R, NonScalarExpr]()
      Sort(x, Fun(ref, f(ref)), ord)

    def limit(lim: Int): Query[R, C] = Limit(x, lim)
    def take(lim: Int): Query[R, C] = limit(lim)

    def offset(lim: Int): Query[R, C] = Offset(x, lim)
    def drop(lim: Int): Query[R, C] = offset(lim)

    def distinct: Query[R, SetResult] = Distinct(x)

    def sum[B: ResultTag](f: Ref[R, NonScalarExpr] => Expr[B, NonScalarExpr]): Aggregation[B] =
      val ref = Ref[R, NonScalarExpr]()
       Aggregation.AggFlatMap(x, Fun(ref, AggregationExpr.Sum(f(ref))))

    def avg[B: ResultTag](f: Ref[R, NonScalarExpr] => Expr[B, NonScalarExpr]): Aggregation[B] =
      val ref = Ref[R, NonScalarExpr]()
       Aggregation.AggFlatMap(x, Fun(ref, AggregationExpr.Avg(f(ref))))

    def max[B: ResultTag](f: Ref[R, NonScalarExpr] => Expr[B, NonScalarExpr]): Aggregation[B] =
      val ref = Ref[R, NonScalarExpr]()
       Aggregation.AggFlatMap(x, Fun(ref, AggregationExpr.Max(f(ref))))

    def min[B: ResultTag](f: Ref[R, NonScalarExpr] => Expr[B, NonScalarExpr]): Aggregation[B] =
      val ref = Ref[R, NonScalarExpr]()
       Aggregation.AggFlatMap(x, Fun(ref, AggregationExpr.Min(f(ref))))

    def size: Aggregation[Int] =
      val ref = Ref[R, ScalarExpr]()
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
    def contains(that: Expr[R, NonScalarExpr]): Expr[Boolean, NonScalarExpr] =
      Expr.Contains(x, that)

    def nonEmpty(): Expr[Boolean, NonScalarExpr] =
      Expr.NonEmpty(x)

    def isEmpty(): Expr[Boolean, NonScalarExpr] =
      Expr.IsEmpty(x)

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
