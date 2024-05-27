package tyql

import language.experimental.namedTuples
import NamedTuple.{AnyNamedTuple, NamedTuple}

/**
 * Shared supertype of query and aggregation
 * @tparam ReturnValue
 */
trait DatabaseAST[ReturnValue]:
  def toSQLString: String = "query"

trait Query[A] extends DatabaseAST[A]:
  /**
   * Top-level queries:
   * map + aggregation => Query[Result], e.g. iterable with length 1
   * flatMap + aggregation = Aggregation[Result], e.g. single return value
   * map + query = Query[Result], e.g. iterable with length n
   * flatMap + query = shouldn't compile
   */
  def flatMap[B](f: Expr.Ref[A] => Query[B]): Query[B] =
    val ref = Expr.Ref[A]()
    Query.FlatMap(this, Expr.Fun(ref, f(ref)))

  def flatMap[B](f: Expr.Ref[A] => Aggregation[B]): Aggregation[B] =
    val ref = Expr.Ref[A]()
    Aggregation.AggFlatMap(this, Expr.Fun(ref, f(ref)))

object Query:
  import Expr.{Pred, Fun, Ref}

  case class Filter[A]($q: Query[A], $p: Pred[A]) extends Query[A]
  case class Map[A, B]($q: Query[A], $f: Fun[A, Expr[B]]) extends Query[B]
  case class FlatMap[A, B]($q: Query[A], $f: Fun[A, Query[B]]) extends Query[B]
  // case class Sort[A]($q: Query[A], $o: Ordering[A]) extends Query[A] // alternative syntax to avoid chaining .sort for multi-key sort
  case class Sort[A, B]($q: Query[A], $f: Fun[A, Expr[B]], $ord: Ord) extends Query[A]
  case class Limit[A]($q: Query[A], $limit: Int) extends Query[A]
  case class Offset[A]($q: Query[A], $offset: Int) extends Query[A]
  case class Drop[A]($q: Query[A], $offset: Int) extends Query[A]
  case class Distinct[A]($q: Query[A]) extends Query[A]

  case class Union[A]($this: Query[A], $other: Query[A], $dedup: Boolean) extends Query[A]
  case class Intersect[A]($this: Query[A], $other: Query[A]) extends Query[A]
  case class Except[A]($this: Query[A], $other: Query[A]) extends Query[A]

  case class Contains[A]($this: Query[A], $other: Expr[A]) extends Expr[Boolean]
  case class IsEmpty[A]($this: Query[A]) extends Expr[Boolean]
  case class NonEmpty[A]($this: Query[A]) extends Expr[Boolean]

  // TODO: spark-style groupBy or SQL groupBy that requires an aggregate?
  // TODO: GroupBy is technically an aggregation but will return an interator of at least 1, like a query
  case class GroupBy[A, B, C]($q: Query[A],
                           $selectFn: Fun[A, Expr[B]],
                           $groupingFn: Fun[A, Expr[C]],
                           $havingFn: Fun[B, Expr[Boolean]]) extends Query[B]

  // Extension methods to support for-expression syntax for queries
  extension [R](x: Query[R])

    def toSQLString: String =
      "query"

    def withFilter(p: Ref[R] => Expr[Boolean]): Query[R] =
      val ref = Ref[R]()
      Filter(x, Fun(ref, p(ref)))

    def filter(p: Ref[R] => Expr[Boolean]): Query[R] = withFilter(p)

    def map[B](f: Ref[R] => Expr[B]): Query[B] =
      val ref = Ref[R]()
      Map(x, Fun(ref, f(ref)))

    def sort[B](f: Ref[R] => Expr[B], ord: Ord): Query[R] =
      val ref = Ref[R]()
      Sort(x, Fun(ref, f(ref)), ord)

    def limit(lim: Int): Query[R] = Limit(x, lim)
    def take(lim: Int): Query[R] = limit(lim)

    def offset(lim: Int): Query[R] = Offset(x, lim)
    def drop(lim: Int): Query[R] = offset(lim)

    def distinct: Query[R] = Distinct(x)

    def sum[B](f: Ref[R] => Expr[B]):Aggregation[B] =
      val ref = Ref[R]()
      Aggregation.AggFlatMap(x, Expr.Fun(ref, Aggregation.Sum(f(ref))))

    def avg[B](f: Ref[R] => Expr[B]):Aggregation[B] =
      val ref = Ref[R]()
       Aggregation.AggFlatMap(x, Expr.Fun(ref, Aggregation.Avg(f(ref))))

    def max[B](f: Ref[R] => Expr[B]):Aggregation[B] =
      val ref = Ref[R]()
       Aggregation.AggFlatMap(x, Expr.Fun(ref, Aggregation.Max(f(ref))))

    def min[B](f: Ref[R] => Expr[B]):Aggregation[B] =
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

    def groupBy[B, C](
     selectFn: Expr.Ref[R] => Expr[C],
     groupingFn: Expr.Ref[R] => Expr[B],
     havingFn: Expr.Ref[C] => Expr[Boolean]
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