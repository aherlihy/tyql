package tyql

import language.experimental.namedTuples
import scala.util.TupledFunction
import NamedTuple.{AnyNamedTuple, NamedTuple}
import scala.annotation.targetName
import java.time.LocalDate
import scala.reflect.ClassTag
import Utils.*
import tyql.Aggregation.AggFilter

trait ResultCategory
class SetResult extends ResultCategory
class BagResult extends ResultCategory

trait Query[A, Category <: ResultCategory](using ResultTag[A]) extends DatabaseAST[A]:
  import Expr.{Pred, Fun, Ref}
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
  def flatMap[B: ResultTag](f: Ref[A, NonScalarExpr] => RestrictedQuery[B, ?]): RestrictedQuery[B, BagResult] =
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

  def fix(p: Query.RestrictedQueryRef[A, ?] => RestrictedQuery[A, SetResult]): Query[A, SetResult] =
    val fn: Tuple1[Query.RestrictedQueryRef[A, ?]] => Tuple1[RestrictedQuery[A, SetResult]] = r => Tuple1(p(r._1))
    Query.fix(Tuple1(this))(fn)._1

  def withFilter(p: Ref[A, NonScalarExpr] => Expr[Boolean, NonScalarExpr]): Query[A, Category] =
    val ref = Ref[A, NonScalarExpr]()
    Query.Filter(this, Fun(ref, p(ref)))

  def filter(p: Ref[A, NonScalarExpr] => Expr[Boolean, NonScalarExpr]): Query[A, Category] = withFilter(p)

// TODO: filter + agg should be technically 'having' but probably don't want to force users to write table.having(...)?
//    def filter(p: Ref[R, ScalarExpr] => Expr[Boolean, ScalarExpr]): Aggregation[R] =
//      val ref = Ref[R, ScalarExpr]()
//       Aggregation.AggFilter(x, Fun(ref, p(ref)))


object Query:
  import Expr.{Pred, Fun, Ref}

//  /** Converts a tuple `(F[T1], ..., F[Tn])` to `(T1,  ... Tn)` */
//  type InverseMap[X <: Tuple, F[_]] <: Tuple = X match {
//    case F[x] *: t => x *: InverseMap[t, F]
//    case EmptyTuple => EmptyTuple
//  }

  /** Given a Tuple `(Query[A], Query[B], ...)`, return `(A, B, ...)` */
  type Elems[QT <: Tuple] = Tuple.InverseMap[QT, [T] =>> Query[T, ?]]

  /**
   * Given a Tuple `(Query[A], Query[B], ...)`, return `(Query[A], Query[B], ...)`
   *
   *  This isn't just the identity because the input might actually be a subtype e.g.
   *  `(Table[A], Table[B], ...)`
   */
  type ToQuery[QT <: Tuple] = Tuple.Map[Elems[QT], [T] =>> Query[T, SetResult]]

  type ToRestrictedQuery[QT <: Tuple] = Tuple.Map[Elems[QT], [T] =>> RestrictedQuery[T, SetResult]]

  /** Given a Tuple `(Query[A], Query[B], ...)`, return `(RestrictedQueryRef[A], RestrictedQueryRef[B], ...)` */
  type ToRestrictedQueryRef[QT <: Tuple] = Tuple.Map[Elems[QT], [T] =>> RestrictedQueryRef[T, SetResult]]

//  def fixUntupled[F, QT <: Tuple](bases: QT)(f: F)(using ev: Tuple.Union[QT] <:< Query[?], tf: TupledFunction[F, ToRestrictedQueryRef[QT] => ToQuery[QT]]): ToQuery[QT] =
//    tf.untupled(multiFix(bases)(ev, tf.tupled))
  /**
   * Fixed point computation.
   *
   * @param bases Tuple of the base case queries. NOTE: bases do not have to be sets since they will be directly fed into an union.
   * TODO: in theory, the results could be assumed to be sets due to the outermost UNION(base, recur), but better to enforce .distinct (also bc we flatten the unions)
   * @param fns A function from a tuple of restricted query refs to a tuple of *SET* restricted queries.
   */
  def fix[QT <: Tuple](bases: QT)(using Tuple.Union[QT] <:< Query[?, ?])(fns: ToRestrictedQueryRef[QT] => ToRestrictedQuery[QT]): ToQuery[QT] =
    // If base cases are themselves recursive definitions.
    val baseRefsAndDefs = bases.toArray.map {
      case MultiRecursive(params, querys, resultQ) => ???// TODO: decide on semantics for multiple fix definitions. (param, query)
      case base: Query[?, ?] => (RestrictedQueryRef()(using base.tag), base)
    }
    val refsTuple = Tuple.fromArray(baseRefsAndDefs.map(_._1)).asInstanceOf[ToRestrictedQueryRef[QT]]
    val refsList = baseRefsAndDefs.map(_._1).toList
    val recurQueries = fns(refsTuple)

    val baseCaseDefsList = baseRefsAndDefs.map(_._2.asInstanceOf[Query[?, ?]])
    val recursiveDefsList: List[Query[?, ?]] = recurQueries.toList.map(_.asInstanceOf[RestrictedQuery[?, ?]].toQuery).lazyZip(baseCaseDefsList).map:
      case (query: Query[t, c], ddef) =>
        // Optimization: remove any extra .distinct calls that are getting fed into a union anyway
        val lhs = ddef match
          case Distinct(from) => from
          case t => t
        val rhs = query match
          case Distinct(from) => from
          case t => t
        Union(lhs.asInstanceOf[Query[t, c]], rhs.asInstanceOf[Query[t, c]])(using query.tag)

    refsTuple.naturalMap([t] => finalRef =>
      val fr = finalRef.asInstanceOf[RestrictedQueryRef[t, ?]]
      given ResultTag[t] = fr.tag
      MultiRecursive(
        refsList,
        recursiveDefsList,
        fr.toQueryRef
      )
    )

  case class MultiRecursive[R]($param: List[RestrictedQueryRef[?, ?]],
                               $subquery: List[Query[?, ?]],
                               $resultQuery: Query[R, ?])(using ResultTag[R]) extends Query[R, SetResult]

  private var refCount = 0
  case class QueryRef[A: ResultTag, C <: ResultCategory]() extends Query[A, C]:
    private val id = refCount
    refCount += 1
    def stringRef() = s"recref$id"
    override def toString: String = s"QueryRef[${stringRef()}]"

  case class RestrictedQueryRef[A: ResultTag, C <: ResultCategory]() extends RestrictedQuery[A, C](QueryRef[A, C]()):
    def toQueryRef: QueryRef[A, C] = wrapped.asInstanceOf[QueryRef[A, C]]

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

  // TODO: also support spark-style groupBy or only SQL groupBy that requires an aggregate operation?
  // TODO: GroupBy is technically an aggregation but will return an interator of at least 1, like a query
  // all attributes in the SELECT clause that are NOT in the scope of an  aggregate function MUST appear in the GROUP BY clause.
  //  case class GroupBy[A, B: ResultTag, C]($q: Query[A],
//                           $selectFn: Fun[A, Expr[B]],
//                           $groupingFn: Fun[A, Expr[C]],
//                           $havingFn: Fun[B, Expr[Boolean]]) extends Query[B]

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
      Aggregation.AggFlatMap(x, Fun(ref, AggregationExpr.Count(ref)))

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
//
//    def groupBy[B, C: ResultTag](
//     selectFn: Expr.Ref[R] => Expr[C],
//     groupingFn: Expr.Ref[R] => Expr[B],
//     havingFn: Expr.Ref[C] => Expr[Boolean] // TODO: make optional
//   ): Query[C] =
//      val ref1 = Expr.Ref[R]()
//      val ref2 = Expr.Ref[R]()
//      val ref3 = Expr.Ref[C]()
//      GroupBy(x, Fun(ref1, selectFn(ref1)), Fun(ref2, groupingFn(ref2)), Fun(ref3, havingFn(ref3)))

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
