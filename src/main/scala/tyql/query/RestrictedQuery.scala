package tyql

import tyql.Utils.{GenerateIndices, HasDuplicate, ZipWithIndex}
import tyql.{DatabaseAST, Expr, NonScalarExpr, Query, ResultTag}

import scala.NamedTuple.AnyNamedTuple
import scala.annotation.{implicitNotFound, targetName}

case class RestrictedQueryRef[A: ResultTag, C <: ResultCategory, ID <: Int](w: Option[Query.QueryRef[A, C]] = None)
    extends RestrictedQuery[A, C, Tuple1[ID]](w.getOrElse(Query.QueryRef[A, C]())):
  type Self = this.type
  def toQueryRef: Query.QueryRef[A, C] = wrapped.asInstanceOf[Query.QueryRef[A, C]]

/** A restricted reference to a query that disallows aggregation. Explicitly do not export aggregate, or any aggregation
  * helpers, exists, etc.
  *
  * Methods can accept RestrictedQuery[A] or Query[A] NOTE: Query[?] indicates no aggregation, but could turn into
  * aggregation, RestrictedQuery[?] means none present and none addable.
  *
  * flatMap/union/unionAll/etc. that accept another RestrictedQuery contain a contextual parameter ev that serves as an
  * affine recursion restriction, e.g. every input relation can only be "depended" on at most once per query since the
  * dependency set parameter `D` must be disjoint.
  */
class RestrictedQuery[A, C <: ResultCategory, D <: Tuple](using ResultTag[A])(protected val wrapped: Query[A, C])
    extends DatabaseAST[A]:
  val tag: ResultTag[A] = qTag
  type deps
  def toQuery: Query[A, C] = wrapped

  // flatMap given a function that returns regular Query does not add any dependencies
  @targetName("restrictedQueryFlatMap")
  def flatMap[B: ResultTag](f: Expr.Ref[A, NonScalarExpr] => Query[B, ?]): RestrictedQuery[B, BagResult, D] =
    RestrictedQuery(wrapped.flatMap(f))

  @targetName("restrictedQueryFlatMapRestricted")
  def flatMap[B: ResultTag, D2 <: Tuple]
    (f: Expr.Ref[A, NonScalarExpr] => RestrictedQuery[B, ?, D2])
//                                        (using @implicitNotFound("Recursive definition must be linearly recursive, e.g. each recursive reference cannot be used twice") ev: Tuple.Disjoint[D, D2] =:= true)
    : RestrictedQuery[B, BagResult, Tuple.Concat[D, D2]] =
    val toR: Expr.Ref[A, NonScalarExpr] => Query[B, ?] = arg => f(arg).toQuery
    RestrictedQuery(wrapped.flatMap(toR))

  def map[B: ResultTag](f: Expr.Ref[A, NonScalarExpr] => Expr[B, NonScalarExpr]): RestrictedQuery[B, BagResult, D] =
    RestrictedQuery(wrapped.map(f))
  def map[B <: AnyNamedTuple : Expr.IsTupleOfExpr]
    (using ResultTag[NamedTuple.Map[B, Expr.StripExpr]])
    (f: Expr.Ref[A, NonScalarExpr] => B)
    : RestrictedQuery[NamedTuple.Map[B, Expr.StripExpr], BagResult, D] = RestrictedQuery(wrapped.map(f))
  def withFilter(p: Expr.Ref[A, NonScalarExpr] => Expr[Boolean, NonScalarExpr]): RestrictedQuery[A, C, D] =
    RestrictedQuery(wrapped.withFilter(p))
  def filter(p: Expr.Ref[A, NonScalarExpr] => Expr[Boolean, NonScalarExpr]): RestrictedQuery[A, C, D] =
    RestrictedQuery(wrapped.filter(p))

  def distinct: RestrictedQuery[A, SetResult, D] = RestrictedQuery(wrapped.distinct)

  def union[D2 <: Tuple]
    (that: RestrictedQuery[A, ?, D2])
//                        (using @implicitNotFound("Recursive definition must be linearly recursive, e.g. each recursive reference cannot be used twice") ev: Tuple.Disjoint[D, D2] =:= true)
    : RestrictedQuery[A, SetResult, Tuple.Concat[D, D2]] =
    RestrictedQuery(Query.Union(wrapped, that.toQuery))

  def unionAll[D2 <: Tuple]
    (that: RestrictedQuery[A, ?, D2])
//                           (using @implicitNotFound("Recursive definition must be linearly recursive, e.g. each recursive reference cannot be used twice") ev: Tuple.Disjoint[D, D2] =:= true)
    : RestrictedQuery[A, BagResult, Tuple.Concat[D, D2]] =
    RestrictedQuery(Query.UnionAll(wrapped, that.toQuery))

  @targetName("unionQuery")
  def union(that: Query[A, ?]): RestrictedQuery[A, SetResult, D] =
    RestrictedQuery(Query.Union(wrapped, that))
  @targetName("unionAllQuery")
  def unionAll(that: Query[A, ?]): RestrictedQuery[A, BagResult, D] =
    RestrictedQuery(Query.UnionAll(wrapped, that))

  def nonEmpty: Expr[Boolean, NonScalarExpr] =
    Expr.NonEmpty(wrapped)

  def isEmpty: Expr[Boolean, NonScalarExpr] =
    Expr.IsEmpty(wrapped)

object RestrictedQuery {

  /** Given a Tuple `(Query[A], Query[B], ...)`, return `(A, B, ...)` */
  type Elems[QT <: Tuple] = Tuple.InverseMap[QT, [T] =>> Query[T, ?]]

  /** Given a Tuple `(Query[A], Query[B], ...)`, return `(Query[A], Query[B], ...)`
    *
    * This isn't just the identity because the input might actually be a subtype e.g. `(Table[A], Table[B], ...)`
    */
  type ToQuery[QT <: Tuple] = Tuple.Map[Elems[QT], [T] =>> Query.MultiRecursive[T]]

  type ConstructRestrictedQuery[T] = T match
    case (t, d) => RestrictedQuery[t, SetResult, d]
  // monotone, linear, and set restricted
  type ToRestrictedQuery[QT <: Tuple, DT <: Tuple] = Tuple.Map[Tuple.Zip[Elems[QT], DT], ConstructRestrictedQuery]
  // only include the monotone restriction, ignore category or linearity
  type ToMonotoneQuery[QT <: Tuple] = Tuple.Map[Elems[QT], [T] =>> RestrictedQuery[T, ?, ?]]
  type ToMonotoneQueryRef[QT <: Tuple] = Tuple.Map[Elems[QT], [T] =>> RestrictedQueryRef[T, ?, ?]]
  // ignore category, linearity, and monotone
  type ToUnrestrictedQuery[QT <: Tuple] = Tuple.Map[Elems[QT], [T] =>> Query[T, ?]]
  type ToUnrestrictedQueryRef[QT <: Tuple] = Tuple.Map[Elems[QT], [T] =>> Query.QueryRef[T, ?]]

  type InverseMapDeps[RQT <: Tuple] <: Tuple = RQT match {
    case RestrictedQuery[a, c, d] *: t => HasDuplicate[d] *: InverseMapDeps[t]
    case EmptyTuple                    => EmptyTuple
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

  type ExtractDependencies[D] <: Tuple = D match
    case RestrictedQuery[a, c, d] => d
  type ExpectedResult[QT <: Tuple] = Tuple.Union[GenerateIndices[0, Tuple.Size[QT]]]
  type ActualResult[RT <: Tuple] = Tuple.Union[Tuple.FlatMap[RT, ExtractDependencies]]

  /** Given a Tuple `(Query[A], Query[B], ...)`, return `(RestrictedQueryRef[A, _, 0], RestrictedQueryRef[B, _, 1],
    * ...)`
    */
  type ToRestrictedQueryRef[QT <: Tuple] = Tuple.Map[
    ZipWithIndex[Elems[QT]],
    [T] =>> T match
      case (elem, index) => RestrictedQueryRef[elem, SetResult, index]
  ]

}
