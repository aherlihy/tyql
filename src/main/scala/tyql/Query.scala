package tyql

import language.experimental.namedTuples
import scala.util.TupledFunction
import NamedTuple.{AnyNamedTuple, NamedTuple}
import scala.compiletime.*
import scala.deriving.Mirror
import java.time.LocalDate
import scala.reflect.ClassTag
import Utils.*
import tyql.Aggregation.AggFilter

type CalculateResult[S, R] <: DatabaseAST[R] = S match
  case ScalarExpr => Aggregation[R]
  case NExpr => Query[R]


enum ResultTag[T]:
  case IntTag extends ResultTag[Int]
  case DoubleTag extends ResultTag[Double]
  case StringTag extends ResultTag[String]
  case BoolTag extends ResultTag[Boolean]
  case LocalDateTag extends ResultTag[LocalDate]
  case NamedTupleTag[N <: Tuple, V <: Tuple](names: List[String], types: List[ResultTag[?]]) extends ResultTag[NamedTuple[N, V]]
  case ProductTag[T](productName: String, fields: ResultTag[NamedTuple.From[T]]) extends ResultTag[T]
  case AnyTag extends ResultTag[Any]
  // TODO: Add more types, specialize for DB backend
object ResultTag:
  given ResultTag[Int] = ResultTag.IntTag
  given ResultTag[String] = ResultTag.StringTag
  given ResultTag[Boolean] = ResultTag.BoolTag
  given ResultTag[Double] = ResultTag.DoubleTag
  given ResultTag[LocalDate] = ResultTag.LocalDateTag
  inline given [N <: Tuple, V <: Tuple]: ResultTag[NamedTuple[N, V]] =
    val names = constValueTuple[N]
    val tpes = summonAll[Tuple.Map[V, ResultTag]]
    NamedTupleTag(names.toList.asInstanceOf[List[String]], tpes.toList.asInstanceOf[List[ResultTag[?]]])

  // We don't really need `fields` and could use `m` for everything, but maybe we can share a cached
  // version of `fields`.
  // Alternatively if we don't care about the case class name we could use only `fields`.
  inline given [T](using m: Mirror.ProductOf[T], fields: ResultTag[NamedTuple.From[T]]): ResultTag[T] =
    val productName = constValue[m.MirroredLabel]
    ProductTag(productName, fields)

/**
 * Shared supertype of query and aggregation
 * @tparam Result
 */
trait DatabaseAST[Result](using val tag: ResultTag[Result]):
  def toSQLString: String = toQueryIR.toSQLString()

  def toQueryIR: QueryIRNode =
    QueryIRTree.generateFullQuery(this, SymbolTable())

trait Aggregation[Result](using override val tag: ResultTag[Result]) extends DatabaseAST[Result] with Expr[Result, NExpr]
object Aggregation:
  case class AggFlatMap[A, B: ResultTag]($from: Query[A], $query: Expr.Fun[A, Expr[B, ?], ?]) extends Aggregation[B]
  case class AggFilter[A: ResultTag]($from: Query[A], $pred: Expr.Pred[A, ScalarExpr]) extends Aggregation[A]

trait Query[A](using ResultTag[A]) extends DatabaseAST[A]:
  /**
   * Classic flatMap with an inner Query that will likely be flattened into a join.
   *
   * @param f    a function that returns a Query (so no aggregations in the subtree)
   * @tparam B   the result type of the query.
   * @return     Query[B], e.g. an iterable of results of type B
   */
  def flatMap[B: ResultTag](f: Expr.Ref[A, NExpr] => Query[B]): Query[B] =
    val ref = Expr.Ref[A, NExpr]()
    Query.FlatMap(this, Expr.Fun(ref, f(ref)))

  /**
   * Equivalent to aggregate(f: Ref => Aggregation). Override to allow this so that aggregations will still work
   * with the for-comprehension syntax. TODO: is this really necessary?
   *
   * @param f   a function that returns an Aggregation (guaranteed agg in subtree)
   * @tparam B  the result type of the aggregation.
   * @return    Aggregation[B], a scalar result, e.g. a single value of type B.
   */
  def flatMap[B: ResultTag](f: Expr.Ref[A, ScalarExpr] => AggregationExpr[B]): Aggregation[B] =
    val ref = Expr.Ref[A, ScalarExpr]()
    Aggregation.AggFlatMap(this, Expr.Fun(ref, f(ref)))


  /**
   * Selectively override to cover common mistakes, so error message is more useful (and not implementation-specific).
   * This is for when a user uses flatMap where they should be using map.
   *
   * @param f a function that returns an Expr, which should be an error, since that only makes sense for map/agg.
   *          TODO: Since Aggregation extends Expr, need to ensure that aggregations don't trigger this.
   */
//  inline def flatMap[B: ResultTag](f: Expr.Ref[A, ?] => Expr[B, ?]): Nothing = // inline so error points to use site
//    error("Cannot return an Expr from a flatMap. Did you mean to use map?")

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
  def aggregate[B: ResultTag](f: Expr.Ref[A, ScalarExpr] => AggregationExpr[B]): Aggregation[B] =
    val ref = Expr.Ref[A, ScalarExpr]()
    Aggregation.AggFlatMap(this, Expr.Fun(ref, f(ref)))

  /**
   * A version of the above-defined aggregate that allows users to skip calling toRow on the result in f.
   *
   * @param f    a function that returns a named-tuple-of-Aggregation.
   * @tparam B   the named-tuple-of-Aggregation that will be converted to an Aggregation-of-named-tuple
   * @return     Aggregation of B.toRow, e.g. a scalar result of type B.toRow
   */
  def aggregate[B <: AnyNamedTuple : AggregationExpr.IsTupleOfAgg](using ResultTag[NamedTuple.Map[B, Expr.StripExpr]])(f: Expr.Ref[A, ScalarExpr] => B): Aggregation[ NamedTuple.Map[B, Expr.StripExpr] ] =
    import AggregationExpr.toRow
    val ref = Expr.Ref[A, ScalarExpr]()
    val row = f(ref).toRow
    Aggregation.AggFlatMap(this, Expr.Fun(ref, row))

  // TODO: bug? if commented out, then agg test cannot find aggregate ^
  inline def aggregate[B: ResultTag](f: Expr.Ref[A, ScalarExpr] => Query[B]): Nothing =
    error("No aggregation function found in f. Did you mean to use flatMap?")

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
   * TODO: ensure that this isn't callable with Aggregation expression.
   */
  def map[B: ResultTag](f: Expr.Ref[A, NExpr] => Expr[B, NExpr]): Query[B] =
    val ref = Expr.Ref[A, NExpr]()
    Query.Map(this, Expr.Fun(ref, f(ref)))

/**
   * A version of the above-defined map that allows users to skip calling toRow on the result in f.
   *
   * @param f    a function that returns a named-tuple-of-Expr.
   * @tparam B   the named-tuple-of-Expr that will be converted to an Expr-of-named-tuple
   * @return     Expr of B.toRow, e.g. an iterable of type B.toRow
   */
  def map[B <: AnyNamedTuple : Expr.IsTupleOfExpr](using ResultTag[NamedTuple.Map[B, Expr.StripExpr]])(f: Expr.Ref[A, NExpr] => B): Query[ NamedTuple.Map[B, Expr.StripExpr] ] =
    import Expr.toRow
    val ref = Expr.Ref[A, NExpr]()
    Query.Map(this, Expr.Fun(ref, f(ref).toRow))

  /**
   * Selectively override to cover common mistakes, so error message is more useful (and not implementation-specific).
   * This is for when a user uses map where they should be using flatMap.
   *
   * @param f   a function that returns an Query, which should be an error, since that only makes sense for flatMap.
   * TODO: Since Aggregation extends Expr, need to ensure that aggregations don't trigger this.
   */
 // inline def map[B: ResultTag](f: Expr.Ref[A, NExpr] => Query[B]): Nothing =
 //   error("Cannot return a Query from a map. Did you mean to use flatMap?")

object Query:
  import Expr.{Pred, Fun, Ref}

  /** Given a Tuple `(Query[A], Query[B], ...)`, return `(A, B, ...)` */
  type Elems[QT <: Tuple] = Tuple.InverseMap[QT, Query]
  /** Given a Tuple `(Query[A], Query[B], ...)`, return `(Query[A], Query[B], ...)`
   *
   *  This isn't just the identity because the input might actually be a subtype e.g.
   *  `(Table[A], Table[B], ...)`
   */
  type ToQuery[QT <: Tuple] = Tuple.Map[Elems[QT], Query]
  /** Given a Tuple `(Query[A], Query[B], ...)`, return `(QueryRef[A], QueryRef[B], ...)` */
  type ToQueryRef[QT <: Tuple] = Tuple.Map[Elems[QT], QueryRef]

//  def fixUntupled[F, QT <: Tuple](bases: QT)(f: F)(using ev: Tuple.Union[QT] <:< Query[?], tf: TupledFunction[F, ToQueryRef[QT] => ToQuery[QT]]): ToQuery[QT] =
//    tf.untupled(multiFix(bases)(ev, tf.tupled))
  /**
   * Fixed point computation.
   */
  def fix[QT <: Tuple](bases: QT)(using Tuple.Union[QT] <:< Query[?])(fns: ToQueryRef[QT] => ToQuery[QT]): ToQuery[QT] =
    val baseRefsAndDefs = bases.toArray.map {
      case MultiRecursive(params, querys, resultQ) => ???//(param, query)
      case base: Query[t] => (QueryRef()(using base.tag), base)
    }
    val refsTuple = Tuple.fromArray(baseRefsAndDefs.map(_._1)).asInstanceOf[ToQueryRef[QT]]
    val refsList = baseRefsAndDefs.map(_._1).toList
    val recurQueries = fns(refsTuple)

    val defsList = baseRefsAndDefs.map(_._2.asInstanceOf[Query[?]])
    val unionsList: List[Query[?]] = recurQueries.toList.lazyZip(defsList).map:
      case (query: Query[t], ddef) =>
        Union(ddef.asInstanceOf[Query[t]], query, false)(using query.tag)

    refsTuple.naturalMap([t] => finalRef =>
      given ResultTag[t] = finalRef.tag
      MultiRecursive(
        refsList,
        unionsList,
        finalRef
      )
    )

  case class MultiRecursive[R]($param: List[QueryRef[?]],
                               $subquery: List[Query[?]],
                               $resultQuery: Query[R])(using ResultTag[R]) extends Query[R]

  private var refCount = 0
  case class QueryRef[A: ResultTag]() extends Query[A]:
    private val id = refCount
    refCount += 1
    def stringRef() = s"recref$id"
    override def toString: String = s"QueryRef[${stringRef()}]"

    override def aggregate[B: ResultTag](f: Expr.Ref[A, ScalarExpr] => AggregationExpr[B]): Aggregation[B] = ???
//    override inline def aggregate[B: ResultTag](f: Expr.Ref[A] => Query[B]): Aggregation[B] = error("Cannot apply aggregation across strata")

//    override def aggregate[B: ResultTag](f: Expr.Ref[A] => Aggregation[B]): Aggregation[B] = reportError()
//
//    override def aggregate[B <: AnyNamedTuple : Aggregation.IsTupleOfAgg]
//    (using ResultTag[NamedTuple.Map[B, Aggregation.StripAgg]])
//    (f: Expr.Ref[A] => B): Aggregation[NamedTuple.Map[B, Aggregation.StripAgg]] = reportError()

//    inline def flatMap[B: ResultTag](f: Expr.Ref[A] => Aggregation[B]): Nothing =


  case class QueryFun[A, B]($param: QueryRef[A], $body: B)

  case class Filter[A: ResultTag]($from: Query[A], $pred: Pred[A, NExpr]) extends Query[A]
  case class Map[A, B: ResultTag]($from: Query[A], $query: Fun[A, Expr[B, ?], ?]) extends Query[B]
  case class FlatMap[A, B: ResultTag]($from: Query[A], $query: Fun[A, Query[B], NExpr]) extends Query[B]
  // case class Sort[A]($q: Query[A], $o: Ordering[A]) extends Query[A] // alternative syntax to avoid chaining .sort for multi-key sort
  case class Sort[A: ResultTag, B]($from: Query[A], $body: Fun[A, Expr[B, NExpr], NExpr], $ord: Ord) extends Query[A]
  case class Limit[A: ResultTag]($from: Query[A], $limit: Int) extends Query[A]
  case class Offset[A: ResultTag]($from: Query[A], $offset: Int) extends Query[A]
  case class Drop[A: ResultTag]($from: Query[A], $offset: Int) extends Query[A]
  case class Distinct[A: ResultTag]($from: Query[A]) extends Query[A]

  case class Union[A: ResultTag]($this: Query[A], $other: Query[A], $dedup: Boolean) extends Query[A]
  case class Intersect[A: ResultTag]($this: Query[A], $other: Query[A]) extends Query[A]
  case class Except[A: ResultTag]($this: Query[A], $other: Query[A]) extends Query[A]

  // TODO: also support spark-style groupBy or only SQL groupBy that requires an aggregate operation?
  // TODO: GroupBy is technically an aggregation but will return an interator of at least 1, like a query
//  case class GroupBy[A, B: ResultTag, C]($q: Query[A],
//                           $selectFn: Fun[A, Expr[B]],
//                           $groupingFn: Fun[A, Expr[C]],
//                           $havingFn: Fun[B, Expr[Boolean]]) extends Query[B]

  // Extension methods to support for-expression syntax for queries
  extension [R: ResultTag](x: Query[R])
    /**
     * When there is only one relation to be defined recursively.
     */
    def fix(p: QueryRef[R] => Query[R]): Query[R] =
      val fn: Tuple1[QueryRef[R]] => Tuple1[Query[R]] = r => Tuple1(p(r._1))
      Query.fix(Tuple1(x))(fn)._1

    def withFilter(p: Ref[R, NExpr] => Expr[Boolean, NExpr]): Query[R] =
      val ref = Ref[R, NExpr]()
      Filter(x, Fun(ref, p(ref)))

    def filter(p: Ref[R, NExpr] => Expr[Boolean, NExpr]): Query[R] = withFilter(p)

    def filter(p: Ref[R, ScalarExpr] => Expr[Boolean, ScalarExpr]): Aggregation[R] =
      val ref = Ref[R, ScalarExpr]()
       Aggregation.AggFilter(x, Fun(ref, p(ref)))

    def sort[B](f: Ref[R, NExpr] => Expr[B, NExpr], ord: Ord): Query[R] =
      val ref = Ref[R, NExpr]()
      Sort(x, Fun(ref, f(ref)), ord)

    def limit(lim: Int): Query[R] = Limit(x, lim)
    def take(lim: Int): Query[R] = limit(lim)

    def offset(lim: Int): Query[R] = Offset(x, lim)
    def drop(lim: Int): Query[R] = offset(lim)

    def distinct: Query[R] = Distinct(x)

    def sum[B: ResultTag](f: Ref[R, ScalarExpr] => Expr[B, ScalarExpr]): Aggregation[B] =
      val ref = Ref[R, ScalarExpr]()
       Aggregation.AggFlatMap(x, Expr.Fun(ref, AggregationExpr.Sum(f(ref))))

//    def avg[B: ResultTag](f: Ref[R] => Expr[B]): Aggregation[B] =
//      val ref = Ref[R]()
//       Aggregation.AggFlatMap(x, Expr.Fun(ref, Aggregation.Avg(f(ref))))
//
//    def max[B: ResultTag](f: Ref[R] => Expr[B]): Aggregation[B] =
//      val ref = Ref[R]()
//       Aggregation.AggFlatMap(x, Expr.Fun(ref, Aggregation.Max(f(ref))))
//
//    def min[B: ResultTag](f: Ref[R] => Expr[B]): Aggregation[B] =
//      val ref = Ref[R]()
//       Aggregation.AggFlatMap(x, Expr.Fun(ref, Aggregation.Min(f(ref))))
//
//    def size: Aggregation[Int] = // TODO: can potentially avoid identity
//      val ref = Ref[R]()
//      Aggregation.AggFlatMap(x, Fun(ref, Aggregation.Count(ref)))

    def union(that: Query[R]): Query[R] =
      Union(x, that, true)

    def unionAll(that: Query[R]): Query[R] =
      Union(x, that, false)

    def intersect(that: Query[R]): Query[R] =
      Intersect(x, that)

    def except(that: Query[R]): Query[R] =
      Except(x, that)

    // Does not work for subsets, need to match types exactly
//    def contains[S <: ScalarExpr](that: Expr[R, S]): Expr[Boolean, S] =
//      Expr.Contains(x, that)

//    def nonEmpty(): Expr[Boolean] =
//      Expr.NonEmpty(x)
//
//    def isEmpty(): Expr[Boolean] =
//      Expr.IsEmpty(x)
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
