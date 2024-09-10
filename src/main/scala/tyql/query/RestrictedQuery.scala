package tyql

import tyql.{DatabaseAST, Expr, NonScalarExpr, Query, ResultTag}

import scala.NamedTuple.AnyNamedTuple
import scala.annotation.targetName

case class RestrictedQueryRef[A: ResultTag, C <: ResultCategory, ID <: Int]() extends RestrictedQuery[A, C, Tuple1[ID]] (Query.QueryRef[A, C]()):
  type Self = this.type
  def toQueryRef: Query.QueryRef[A, C] = wrapped.asInstanceOf[Query.QueryRef[A, C]]

trait RestrictedQuery1[B]:
  self =>
  type Deps = B
  
  def flatMap[B1](q: RestrictedQuery1[B1]): RestrictedQuery1[(B, B1)] = new RestrictedQuery1[(B, B1)]:
    type Deps = (self.Deps, q.Deps)

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
class RestrictedQuery[A, C <: ResultCategory, D <: Tuple](using ResultTag[A])(protected val wrapped: Query[A, C]) extends DatabaseAST[A]:
  val tag: ResultTag[A] = qTag
  type deps
  def toQuery: Query[A, C] = wrapped

  // flatMap given a function that returns regular Query does not add any dependencies
  @targetName("restrictedQueryFlatMap")
  def flatMap[B: ResultTag](f: Expr.Ref[A, NonScalarExpr] => Query[B, ?]): RestrictedQuery[B, BagResult, D] = RestrictedQuery(wrapped.flatMap(f))

  @targetName("restrictedQueryFlatMapRestricted")
  def flatMap[B: ResultTag, D2 <: Tuple](f: Expr.Ref[A, NonScalarExpr] => RestrictedQuery[B, ?, D2])(using ev: Tuple.Disjoint[D, D2] =:= true): RestrictedQuery[B, BagResult, Tuple.Concat[D, D2]] =
    val toR: Expr.Ref[A, NonScalarExpr] => Query[B, ?] = arg => f(arg).toQuery
    RestrictedQuery(wrapped.flatMap(toR))

  def map[B: ResultTag](f: Expr.Ref[A, NonScalarExpr] => Expr[B, NonScalarExpr]): RestrictedQuery[B, BagResult, D] = RestrictedQuery(wrapped.map(f))
  def map[B <: AnyNamedTuple : Expr.IsTupleOfExpr](using ResultTag[NamedTuple.Map[B, Expr.StripExpr]])(f: Expr.Ref[A, NonScalarExpr] => B): RestrictedQuery[NamedTuple.Map[B, Expr.StripExpr], BagResult, D] = RestrictedQuery(wrapped.map(f))
  def withFilter(p: Expr.Ref[A, NonScalarExpr] => Expr[Boolean, NonScalarExpr]): RestrictedQuery[A, C, D] = RestrictedQuery(wrapped.withFilter(p))
  def filter(p: Expr.Ref[A, NonScalarExpr] => Expr[Boolean, NonScalarExpr]): RestrictedQuery[A, C, D] = RestrictedQuery(wrapped.filter(p))

  def distinct: RestrictedQuery[A, SetResult, D] = RestrictedQuery(wrapped.distinct)

  def union[D2 <: Tuple](that: RestrictedQuery[A, ?, D2])(using ev: Tuple.Disjoint[D, D2] =:= true): RestrictedQuery[A, SetResult, Tuple.Concat[D, D2]] =
    RestrictedQuery(Query.Union(wrapped, that.toQuery))

  def unionAll[D2 <: Tuple](that: RestrictedQuery[A, ?, D2])(using ev: Tuple.Disjoint[D, D2] =:= true): RestrictedQuery[A, BagResult, Tuple.Concat[D, D2]] =
    RestrictedQuery(Query.UnionAll(wrapped, that.toQuery))

  @targetName("unionQuery")
  def union(that: Query[A, ?]): RestrictedQuery[A, SetResult, D] =
    RestrictedQuery(Query.Union(wrapped, that))
  @targetName("unionAllQuery")
  def unionAll(that: Query[A, ?]): RestrictedQuery[A, BagResult, D] =
    RestrictedQuery(Query.UnionAll(wrapped, that))

  // TODO: Does nonEmpty count as non-monotone? (yes)
  def nonEmpty: Expr[Boolean, NonScalarExpr] =
    Expr.NonEmpty(wrapped)

  def isEmpty: Expr[Boolean, NonScalarExpr] =
    Expr.IsEmpty(wrapped)


