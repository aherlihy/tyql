package tyql

import tyql.Utils.{GenerateIndices, HasDuplicate, ZipWithIndex}
import tyql.{DatabaseAST, Expr, NonScalarExpr, Query, ResultTag}

import scala.NamedTuple.AnyNamedTuple
import scala.annotation.{implicitNotFound, targetName}

case class RestrictedQueryRef[A: ResultTag, C <: ResultCategory, ID <: Int, RestrictionCF <: ConstructorFreedom](w: Option[Query.QueryRef[A, C]] = None) extends RestrictedQuery[A, C, Tuple1[ID], RestrictionCF] (w.getOrElse(Query.QueryRef[A, C]())):
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
class RestrictedQuery[A, C <: ResultCategory, D <: Tuple, RestrictionCF <: ConstructorFreedom](using ResultTag[A])(protected val wrapped: Query[A, C]) extends DatabaseAST[A]:
  val tag: ResultTag[A] = qTag
  type deps
  def toQuery: Query[A, C] = wrapped

  // flatMap given a function that returns regular Query does not add any dependencies
  @targetName("restrictedQueryFlatMap")
  def flatMap[B: ResultTag](f: Expr.Ref[A, NonScalarExpr, RestrictionCF] => Query[B, ?]): RestrictedQuery[B, BagResult, D, RestrictionCF] =
    val ref = Expr.Ref[A, NonScalarExpr, RestrictionCF]()
    RestrictedQuery(Query.FlatMap(wrapped, Expr.Fun(ref, f(ref))))

  @targetName("restrictedQueryFlatMapRestricted")
  def flatMap[B: ResultTag, D2 <: Tuple](f: Expr.Ref[A, NonScalarExpr, RestrictionCF] => RestrictedQuery[B, ?, D2, RestrictionCF]): RestrictedQuery[B, BagResult, Tuple.Concat[D, D2], RestrictionCF] =
//  (using @implicitNotFound("Recursive definition must be linearly recursive, e.g. each recursive reference cannot be used twice") ev: Tuple.Disjoint[D, D2] =:= true)
    val toR: Expr.Ref[A, NonScalarExpr, RestrictionCF] => Query[B, ?] = arg => f(arg).toQuery
    val ref = Expr.Ref[A, NonScalarExpr, RestrictionCF]()
    RestrictedQuery(Query.FlatMap(wrapped, Expr.Fun(ref, toR(ref))))

  def map[B: ResultTag, CF <: ConstructorFreedom](f: Expr.Ref[A, NonScalarExpr, RestrictionCF] => Expr[B, NonScalarExpr, CF]): RestrictedQuery[B, BagResult, D, RestrictionCF] =
    val ref = Expr.Ref[A, NonScalarExpr, RestrictionCF]()
    RestrictedQuery(Query.Map(wrapped, Expr.Fun(ref, f(ref))))

  def map[B <: AnyNamedTuple: Expr.IsTupleOfExpr](using ResultTag[NamedTuple.Map[B, Expr.StripExpr]])(f: Expr.Ref[A, NonScalarExpr, RestrictionCF] => B): RestrictedQuery[NamedTuple.Map[B, Expr.StripExpr], BagResult, D, RestrictionCF] =
    import Expr.toRow
    val ref = Expr.Ref[A, NonScalarExpr, RestrictionCF]()
    RestrictedQuery(Query.Map(wrapped, Expr.Fun(ref, f(ref).toRow)))

  def withFilter[CF <: ConstructorFreedom](p: Expr.Ref[A, NonScalarExpr, RestrictionCF] => Expr[Boolean, NonScalarExpr, CF]): RestrictedQuery[A, C, D, RestrictionCF] =
    val ref = Expr.Ref[A, NonScalarExpr, RestrictionCF]()
    RestrictedQuery(Query.Filter[A, C](wrapped, Expr.Fun(ref, p(ref))))
  def filter[CF <: ConstructorFreedom](p: Expr.Ref[A, NonScalarExpr, RestrictionCF] => Expr[Boolean, NonScalarExpr, CF]): RestrictedQuery[A, C, D, RestrictionCF] =
    val ref = Expr.Ref[A, NonScalarExpr, RestrictionCF]()
    RestrictedQuery(Query.Filter[A, C](wrapped, Expr.Fun(ref, p(ref))))

  def distinct: RestrictedQuery[A, SetResult, D, RestrictionCF] = RestrictedQuery(wrapped.distinct)

  def union[D2 <: Tuple](that: RestrictedQuery[A, ?, D2, RestrictionCF])
//                        (using @implicitNotFound("Recursive definition must be linearly recursive, e.g. each recursive reference cannot be used twice") ev: Tuple.Disjoint[D, D2] =:= true)
    : RestrictedQuery[A, SetResult, Tuple.Concat[D, D2], RestrictionCF] =
    RestrictedQuery(Query.Union(wrapped, that.toQuery))

  def unionAll[D2 <: Tuple](that: RestrictedQuery[A, ?, D2, RestrictionCF])
//                           (using @implicitNotFound("Recursive definition must be linearly recursive, e.g. each recursive reference cannot be used twice") ev: Tuple.Disjoint[D, D2] =:= true)
    : RestrictedQuery[A, BagResult, Tuple.Concat[D, D2], RestrictionCF] =
    RestrictedQuery(Query.UnionAll(wrapped, that.toQuery))

  @targetName("unionQuery")
  def union(that: Query[A, ?]): RestrictedQuery[A, SetResult, D, RestrictionCF] =
    RestrictedQuery(Query.Union(wrapped, that))
  @targetName("unionAllQuery")
  def unionAll(that: Query[A, ?]): RestrictedQuery[A, BagResult, D, RestrictionCF] =
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

  type ConstructRestrictedQuery[T, CF <: ConstructorFreedom] = T match
    case (t, d) => RestrictedQuery[t, SetResult, d, CF]
  // monotone, linear, set restricted, and constructor-free (arg)
  type ToRestrictedQuery[QT <: Tuple, DT <: Tuple, RCF <: ConstructorFreedom] = Tuple.Map[Tuple.Zip[Elems[QT], DT], [T] =>> ConstructRestrictedQuery[T, RCF]]
  type ToRestrictedQueryRef[QT <: Tuple, RCF <: ConstructorFreedom] = Tuple.Map[ZipWithIndex[Elems[QT]], [T] =>> T match
    case (elem, index) => RestrictedQueryRef[elem, SetResult, index, RCF]]
  // monotone, linear, set restricted, and NOT constructor-free
  type ToRestrictedQueryNCF[QT <: Tuple, DT <: Tuple] = Tuple.Map[Tuple.Zip[Elems[QT], DT], [T] =>> ConstructRestrictedQuery[T, NonRestrictedConstructors]]
  // only include the monotone restriction, ignore category or linearity
  type ToMonotoneQuery[QT <: Tuple] = Tuple.Map[Elems[QT], [T] =>> RestrictedQuery[T, ?, ?, NonRestrictedConstructors]]
  type ToMonotoneQueryRef[QT <: Tuple] = Tuple.Map[Elems[QT], [T] =>> RestrictedQueryRef[T, ?, ?, NonRestrictedConstructors]]
  // ignore category, linearity, and monotone
  type ToUnrestrictedQuery[QT <: Tuple] = Tuple.Map[Elems[QT], [T] =>> Query[T, ?]]
  type ToUnrestrictedQueryRef[QT <: Tuple] = Tuple.Map[Elems[QT], [T] =>> Query.QueryRef[T, ?]]

  type InverseMapDeps[RQT <: Tuple] <: Tuple = RQT match {
    case RestrictedQuery[a, c, d, rcf] *: t => HasDuplicate[d] *: InverseMapDeps[t]
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

  type ExtractDependencies[D] <: Tuple = D match
    case RestrictedQuery[a, c, d, rcf] => d
  type ExpectedResult[QT <: Tuple] = Tuple.Union[GenerateIndices[0, Tuple.Size[QT]]]
  type ActualResult[RT <: Tuple] = Tuple.Union[Tuple.FlatMap[RT, ExtractDependencies]]


}
