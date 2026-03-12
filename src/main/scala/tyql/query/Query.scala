package tyql

import language.experimental.namedTuples
import NamedTuple.{AnyNamedTuple, NamedTuple}
import scala.annotation.{implicitNotFound, targetName}
import scala.reflect.ClassTag
import tyql.Expr.{IsTupleOfExpr, StripExpr}

trait Query[A](using ResultTag[A]) extends DatabaseAST[A]:
  import Expr.{Fun, Ref}
  val tag: ResultTag[A] = qTag
  /**
   * Classic flatMap with an inner Query that will likely be flattened into a join.
   *
   * @param f    a function that returns a Query (so no aggregations in the subtree)
   * @tparam B   the result type of the query.
   * @return     Query[B], e.g. an iterable of results of type B
   */
  def flatMap[B: ResultTag](f: Ref[A] => Query[B]): Query[B] =
    val ref = Ref[A]()
    Query.FlatMap(this, Fun(ref, f(ref)))

  /**
   * Equivalent to flatMap(f: Ref => Aggregation).
   * NOTE: make Ref of type NExpr so that relation.id is counted as NExpr, not ScalarExpr
   *
   * @param f   a function that returns an Aggregation (guaranteed agg in subtree)
   * @tparam B  the result type of the aggregation.
   * @return    Aggregation[B], a scalar result, e.g. a single value of type B.
   */
  def aggregate[B: ResultTag, T <: Tuple](f: Ref[A] => Aggregation[T, B]): Aggregation[A *: T, B] =
    val ref = Ref[A]()
    Aggregation.AggFlatMap[A *: T, B](this, Fun(ref, f(ref)))

  /**
   * Equivalent version of map(f: Ref => AggregationExpr).
   * Requires f to call toRow on the final result before returning.
   * Sometimes the implicit conversion kicks in and converts a named-tuple-of-Agg into a Agg-of-named-tuple,
   * but not always. As an alternativean use the aggregate defined below that explicitly calls toRow on the result of f.
   *
   * @param f   a function that returns an Aggregation (guaranteed agg in subtree)
   * @tparam B  the result type of the aggregation.
   * @return    Aggregation[B], a scalar result, e.g. single value of type B.
   */
  @targetName("AggregateExpr")
  def aggregate[B: ResultTag](f: Ref[A] => AggregationExpr[B]): Aggregation[A *: EmptyTuple, B] =
    val ref = Ref[A]()
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
    (f: Ref[A] => B)
  : Aggregation[A *: EmptyTuple, NamedTuple.Map[B, Expr.StripExpr] ] =

    import AggregationExpr.toRow
    val ref = Ref[A]()
    val row = f(ref).toRow
    Aggregation.AggFlatMap[A *: EmptyTuple, NamedTuple.Map[B, Expr.StripExpr] ](this, Fun(ref, row.asInstanceOf[Expr[NamedTuple.Map[B, Expr.StripExpr]]]))

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
                                          groupingFn: Ref[A] => Expr[GroupResult],
                                          selectFn: Ref[A] => Expr[R]
                                          //  (using ev: Tuple.Union[GetFields[A]] <:< Tuple.Union[GetFields[G]])
   ): Query.GroupBy[A, R, GroupResult] =
    val refG = Ref[A]()
    val groupFun = Fun(refG, groupingFn(refG))

    val refS = Ref[A]()
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
                                                   groupingFn: Ref[A] => Expr[GroupResult],
                                                   selectFn: Ref[A] => Expr[R]
  ): Query.GroupBy[A, R, GroupResult] =
    val refG = Ref[A]()
    val groupFun = Fun(refG, groupingFn(refG))

    val refS = Ref[A]()
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
                                                  groupingFn: Ref[A] => Expr[GroupResult],
                                                  selectFn: Ref[A] => Expr[R],
                                                  havingFn: Ref[A] => Expr[Boolean]
                                        ): Query.GroupBy[A, R, GroupResult] =
    val refG = Ref[A]()
    val groupFun = Fun(refG, groupingFn(refG))

    val refS = Ref[A]()
    val selectFun = Fun(refS, selectFn(refS))

    // Cast is workaround for: "ScalarExpr is not subtype of ?"
    Query.GroupBy(this, groupFun, selectFun, None).having(havingFn).asInstanceOf[Query.GroupBy[A, R, GroupResult]]

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
   * but not always. As an alternativean use the map defined below that explicitly calls toRow on the result of f.
   *
   * @param f     a function that returns a Expression.
   * @tparam B    the result type of the Expression, and resulting query.
   * @return      Query[B], an iterable of type B.
   *
   */
  def map[B: ResultTag](f: Ref[A] => Expr[B]): Query[B] =
    val ref = Ref[A]()
    Query.Map(this, Fun(ref, f(ref)))

  /**
   * A version of the above-defined map that allows users to skip calling toRow on the result in f.
   *
   * @param f    a function that returns a named-tuple-of-Expr.
   * @tparam B   the named-tuple-of-Expr that will be converted to an Expr-of-named-tuple
   * @return     Expr of B.toRow, e.g. an iterable of type B.toRow
   */
  def map[B <: AnyNamedTuple: IsTupleOfExpr](using ResultTag[NamedTuple.Map[B, Expr.StripExpr]])(f: Ref[A] => B): Query[ NamedTuple.Map[B, Expr.StripExpr] ] =
    import Expr.toRow
    val ref = Ref[A]()
    Query.Map(this, Fun(ref, f(ref).toRow))

  def withFilter(p: Ref[A] => Expr[Boolean]): Query[A] =
    val ref = Ref[A]()
    Query.Filter[A](this, Fun(ref, p(ref)))

  def filter(p: Ref[A] => Expr[Boolean]): Query[A] =
    withFilter(p)

  def nonEmpty: Expr[Boolean] =
    Expr.NonEmpty(this)

  def isEmpty: Expr[Boolean] =
    Expr.IsEmpty(this)

object Query:
  import Expr.{Fun, Ref}

  private var refCount = 0
  case class QueryRef[A: ResultTag]() extends Query[A]:
    private val id = refCount
    refCount += 1
    def stringRef() = s"recref$id"
    override def toString: String = s"QueryRef[${stringRef()}]"

  case class QueryFun[A, B]($param: QueryRef[A], $body: B)

  case class Filter[A: ResultTag]($from: Query[A], $pred: Fun[A, Expr[Boolean]]) extends Query[A]
  case class Map[A, B: ResultTag]($from: Query[A], $query: Fun[A, Expr[B]]) extends Query[B]
  case class FlatMap[A, B: ResultTag]($from: Query[A], $query: Fun[A, Query[B]]) extends Query[B]
  // case class Sort[A]($q: Query[A], $o: Ordering[A]) extends Query[A] // alternative syntax to avoid chaining .sort for multi-key sort
  case class Sort[A: ResultTag, B]($from: Query[A], $body: Fun[A, Expr[B]], $ord: Ord) extends Query[A]
  case class Limit[A: ResultTag]($from: Query[A], $limit: Int) extends Query[A]
  case class Offset[A: ResultTag]($from: Query[A], $offset: Int) extends Query[A]
  case class Drop[A: ResultTag]($from: Query[A], $offset: Int) extends Query[A]
  case class Distinct[A: ResultTag]($from: Query[A]) extends Query[A]

  case class Union[A: ResultTag]($this: Query[A], $other: Query[A]) extends Query[A]
  case class UnionAll[A: ResultTag]($this: Query[A], $other: Query[A]) extends Query[A]
  case class Intersect[A: ResultTag]($this: Query[A], $other: Query[A]) extends Query[A]
  case class IntersectAll[A: ResultTag]($this: Query[A], $other: Query[A]) extends Query[A]
  case class Except[A: ResultTag]($this: Query[A], $other: Query[A]) extends Query[A]
  case class ExceptAll[A: ResultTag]($this: Query[A], $other: Query[A]) extends Query[A]

  case class NewGroupBy[
    AllSourceTypes <: Tuple,
    ResultType: ResultTag,
    GroupingType,
  ]($source: Aggregation[AllSourceTypes, ResultType],
    $grouping: Expr[GroupingType],
    $sourceRefs: Seq[Ref[?]],
    $sourceTags: collection.Seq[(String, ResultTag[?])],
    $having: Option[Expr[Boolean]]) extends Query[ResultType]:
    /**
     * Don't overload filter because having operates on the pre-grouped type.
     */
    def having(havingFn: ToRef[AllSourceTypes] => Expr[Boolean]): Query[ResultType] =
      if ($having.isEmpty)
        val refsTuple = Tuple.fromArray($sourceRefs.toArray).asInstanceOf[ToRef[AllSourceTypes]]

        val havingResult = havingFn(refsTuple)
        NewGroupBy($source, $grouping, $sourceRefs, $sourceTags, Some(havingResult))
      else
        throw new Exception("Error: can only support a single having statement after groupBy")

  // NOTE: GroupBy is technically an aggregation but will return an interator of at least 1, like a query
  case class GroupBy[
    SourceType,
    ResultType: ResultTag,
    GroupingType,
  ]($source: Query[SourceType],
    $groupingFn: Fun[SourceType, Expr[GroupingType]],
    $selectFn: Fun[SourceType, Expr[ResultType]],
    $havingFn: Option[Fun[SourceType, Expr[Boolean]]]) extends Query[ResultType]:
    /**
     * Don't overload filter because having operates on the pre-grouped type.
     */
    def having(p: Ref[SourceType] => Expr[Boolean]): Query[ResultType] =
      if ($havingFn.isEmpty)
        val ref = Ref[SourceType]()(using $source.tag)
        val fun = Fun(ref, p(ref))
        GroupBy($source, $groupingFn, $selectFn, Some(fun))
      else
        throw new Exception("Error: can only support a single having statement after groupBy")

  // Extensions. TODO: Any reason not to move these into Query methods?
  extension [R: ResultTag](x: Query[R])
    /**
     * When there is only one relation to be defined recursively.
     */
    def sort[B](f:Ref[R] => Expr[B], ord: Ord): Query[R] =
      val ref =Ref[R]()
      Sort(x, Fun(ref, f(ref)), ord)

    def limit(lim: Int): Query[R] = Limit(x, lim)
    def take(lim: Int): Query[R] = limit(lim)

    def offset(lim: Int): Query[R] = Offset(x, lim)
    def drop(lim: Int): Query[R] = offset(lim)

    def distinct: Query[R] = Distinct(x)

    def sum[B: ResultTag](f:Ref[R] => Expr[B]): Aggregation[R *: EmptyTuple, B] =
      val ref =Ref[R]()
       Aggregation.AggFlatMap(x, Fun(ref, AggregationExpr.Sum(f(ref))))

    def avg[B: ResultTag](f:Ref[R] => Expr[B]): Aggregation[R *: EmptyTuple, B] =
      val ref =Ref[R]()
       Aggregation.AggFlatMap(x, Fun(ref, AggregationExpr.Avg(f(ref))))

    def max[B: ResultTag](f:Ref[R] => Expr[B]): Aggregation[R *: EmptyTuple, B] =
      val ref =Ref[R]()
       Aggregation.AggFlatMap(x, Fun(ref, AggregationExpr.Max(f(ref))))

    def min[B: ResultTag](f:Ref[R] => Expr[B]): Aggregation[R *: EmptyTuple, B] =
      val ref =Ref[R]()
       Aggregation.AggFlatMap(x, Fun(ref, AggregationExpr.Min(f(ref))))

    def size: Aggregation[R *: EmptyTuple, Int] =
      val ref =Ref[R]()
      Aggregation.AggFlatMap(x, Fun(ref, AggregationExpr.Count(Expr.IntLit(1))))

    def union(that: Query[R]): Query[R] =
      Union(x, that)

    def unionAll(that: Query[R]): Query[R] =
      UnionAll(x, that)

    def intersect(that: Query[R]): Query[R] =
      Intersect(x, that)

    def intersectAll(that: Query[R]): Query[R] =
      IntersectAll(x, that)

    def except(that: Query[R]): Query[R] =
      Except(x, that)

    def exceptAll(that: Query[R]): Query[R] =
      ExceptAll(x, that)

    // Does not work for subsets, need to match types exactly
    def contains(that: Expr[R]): Expr[Boolean] =
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
