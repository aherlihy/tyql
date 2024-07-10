package tyql

import language.experimental.namedTuples
import NamedTuple.{AnyNamedTuple, NamedTuple}
import scala.compiletime.*
import scala.deriving.Mirror
import java.time.LocalDate

enum ResultTag[T]:
  case IntTag extends ResultTag[Int]
  case DoubleTag extends ResultTag[Double]
  case StringTag extends ResultTag[String]
  case BoolTag extends ResultTag[Boolean]
  case LocalDateTag extends ResultTag[LocalDate]
  case NamedTupleTag[N <: Tuple, V <: Tuple](names: List[String], types: List[ResultTag[?]]) extends ResultTag[NamedTuple[N, V]]
  case ProductTag[T](productName: String, fields: ResultTag[NamedTuple.From[T]]) extends ResultTag[T]
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

  private def toQueryIR: QueryIRNode =
    QueryIRTree.generateTopQuery(this)

trait Query[A](using ResultTag[A]) extends DatabaseAST[A]:
  /**
   * Top-level queries:
   * map + aggregation => Aggregation[Result]
   * flatMap + aggregation => Aggregation[Result]
   * map + query => Query[Result], e.g. iterable with length n
   * flatMap + query = compilation error
   */
  def flatMap[B: ResultTag](f: Expr.Ref[A] => Query[B]): Query[B] =
    val ref = Expr.Ref[A]()
    Query.FlatMap(this, Expr.Fun(ref, f(ref)))

  def flatMap[B: ResultTag](f: Expr.Ref[A] => Aggregation[B]): Aggregation[B] =
    val ref = Expr.Ref[A]()
    Aggregation.AggFlatMap(this, Expr.Fun(ref, f(ref)))

  // TODO: cover common cases so that user doesn't see implementation-specific typing error, e.g.
  inline def flatMap[B: ResultTag](f: Expr.Ref[A] => Expr[B]): Expr[B] = // inline so error points to use site
    error("Cannot return an Expr from a flatMap. Did you mean to use map?")

  def aggregate[B: ResultTag](f: Expr.Ref[A] => Aggregation[B]): Aggregation[B] =
    val ref = Expr.Ref[A]()
    Aggregation.AggFlatMap(this, Expr.Fun(ref, f(ref)))

  def aggregate[B <: AnyNamedTuple : Aggregation.IsTupleOfAgg](using ResultTag[NamedTuple.Map[B, Aggregation.StripAgg]])(f: Expr.Ref[A] => B): Aggregation[ NamedTuple.Map[B, Aggregation.StripAgg] ] =
    import Aggregation.toRow
    val ref = Expr.Ref[A]()
    val row = f(ref).toRow
    Aggregation.AggFlatMap(this, Expr.Fun(ref, row))

  // TODO: bug? if commented out, then agg test cannot find aggregate ^
  inline def aggregate[B: ResultTag](f: Expr.Ref[A] => Query[B]): Nothing =
    error("Cannot return an Query from a aggregate. Did you mean to use flatMap?")

  def map[B: ResultTag](f: Expr.Ref[A] => Expr[B]): Query[B] =
    val ref = Expr.Ref[A]()
    Query.Map(this, Expr.Fun(ref, f(ref)))

  def map[B <: AnyNamedTuple : Expr.IsTupleOfExpr](using ResultTag[NamedTuple.Map[B, Expr.StripExpr]])(f: Expr.Ref[A] => B): Query[ NamedTuple.Map[B, Expr.StripExpr] ] =
    import Expr.toRow
    val ref = Expr.Ref[A]()
    Query.Map(this, Expr.Fun(ref, f(ref).toRow))

  inline def map[B: ResultTag](f: Expr.Ref[A] => Query[B]): Nothing =
    error("Cannot return an Query from a map. Did you mean to use flatMap?")

object Query:
  import Expr.{Pred, Fun, Ref}

  case class Filter[A: ResultTag]($from: Query[A], $pred: Pred[A]) extends Query[A]
  case class Map[A, B: ResultTag]($from: Query[A], $query: Fun[A, Expr[B]]) extends Query[B]
  case class FlatMap[A, B: ResultTag]($from: Query[A], $query: Fun[A, Query[B]]) extends Query[B]
  // case class Sort[A]($q: Query[A], $o: Ordering[A]) extends Query[A] // alternative syntax to avoid chaining .sort for multi-key sort
  case class Sort[A: ResultTag, B]($q: Query[A], $f: Fun[A, Expr[B]], $ord: Ord) extends Query[A]
  case class Limit[A: ResultTag]($q: Query[A], $limit: Int) extends Query[A]
  case class Offset[A: ResultTag]($q: Query[A], $offset: Int) extends Query[A]
  case class Drop[A: ResultTag]($q: Query[A], $offset: Int) extends Query[A]
  case class Distinct[A: ResultTag]($q: Query[A]) extends Query[A]

  case class Union[A: ResultTag]($this: Query[A], $other: Query[A], $dedup: Boolean) extends Query[A]
  case class Intersect[A: ResultTag]($this: Query[A], $other: Query[A]) extends Query[A]
  case class Except[A: ResultTag]($this: Query[A], $other: Query[A]) extends Query[A]

  case class Contains[A]($this: Query[A], $other: Expr[A]) extends Expr[Boolean]
  case class IsEmpty[A]($this: Query[A]) extends Expr[Boolean]
  case class NonEmpty[A]($this: Query[A]) extends Expr[Boolean]

  // TODO: also support spark-style groupBy or only SQL groupBy that requires an aggregate operation?
  // TODO: GroupBy is technically an aggregation but will return an interator of at least 1, like a query
  case class GroupBy[A, B: ResultTag, C]($q: Query[A],
                           $selectFn: Fun[A, Expr[B]],
                           $groupingFn: Fun[A, Expr[C]],
                           $havingFn: Fun[B, Expr[Boolean]]) extends Query[B]

  // Extension methods to support for-expression syntax for queries
  extension [R: ResultTag](x: Query[R])

    def withFilter(p: Ref[R] => Expr[Boolean]): Query[R] =
      val ref = Ref[R]()
      Filter(x, Fun(ref, p(ref)))

    def filter(p: Ref[R] => Expr[Boolean]): Query[R] = withFilter(p)

    def sort[B](f: Ref[R] => Expr[B], ord: Ord): Query[R] =
      val ref = Ref[R]()
      Sort(x, Fun(ref, f(ref)), ord)

    def limit(lim: Int): Query[R] = Limit(x, lim)
    def take(lim: Int): Query[R] = limit(lim)

    def offset(lim: Int): Query[R] = Offset(x, lim)
    def drop(lim: Int): Query[R] = offset(lim)

    def distinct: Query[R] = Distinct(x)

    def sum[B: ResultTag](f: Ref[R] => Expr[B]): Aggregation[B] =
      val ref = Ref[R]()
      Aggregation.AggFlatMap(x, Expr.Fun(ref, Aggregation.Sum(f(ref))))

    def avg[B: ResultTag](f: Ref[R] => Expr[B]): Aggregation[B] =
      val ref = Ref[R]()
       Aggregation.AggFlatMap(x, Expr.Fun(ref, Aggregation.Avg(f(ref))))

    def max[B: ResultTag](f: Ref[R] => Expr[B]): Aggregation[B] =
      val ref = Ref[R]()
       Aggregation.AggFlatMap(x, Expr.Fun(ref, Aggregation.Max(f(ref))))

    def min[B: ResultTag](f: Ref[R] => Expr[B]): Aggregation[B] =
      val ref = Ref[R]()
       Aggregation.AggFlatMap(x, Expr.Fun(ref, Aggregation.Min(f(ref))))

    def size: Aggregation[Int] = // TODO: can potentially avoid identity
      val ref = Ref[R]()
      Aggregation.AggFlatMap(x, Fun(ref, Aggregation.Count(ref)))

    def union(that: Query[R]): Query[R] =
      Union(x, that, true)

    def unionAll(that: Query[R]): Query[R] =
      Union(x, that, false)

    def intersect(that: Query[R]): Query[R] =
      Intersect(x, that)

    def except(that: Query[R]): Query[R] =
      Except(x, that)

    // Does not work for subsets, need to match types exactly
    def contains(that: Expr[R]): Expr[Boolean] =
      Contains(x, that)

    def nonEmpty(): Expr[Boolean] =
      NonEmpty(x)

    def isEmpty(): Expr[Boolean] =
      IsEmpty(x)

    def groupBy[B, C: ResultTag](
     selectFn: Expr.Ref[R] => Expr[C],
     groupingFn: Expr.Ref[R] => Expr[B],
     havingFn: Expr.Ref[C] => Expr[Boolean] // TODO: make optional
   ): Query[C] =
      val ref1 = Expr.Ref[R]()
      val ref2 = Expr.Ref[R]()
      val ref3 = Expr.Ref[C]()
      GroupBy(x, Fun(ref1, selectFn(ref1)), Fun(ref2, groupingFn(ref2)), Fun(ref3, havingFn(ref3)))

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
