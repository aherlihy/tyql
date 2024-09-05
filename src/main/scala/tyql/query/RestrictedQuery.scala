package tyql

import tyql.{DatabaseAST, Expr, NonScalarExpr, Query, ResultTag}

import scala.NamedTuple.AnyNamedTuple
import scala.annotation.targetName

/**
 * A restricted reference to a query that disallows aggregation.
 * Explicitly do not export aggregate, or any aggregation helpers, exists, etc.
 *
 * Methods can accept RestrictedQuery[A] or Query[A]
 * NOTE: Query[?] indicates no aggregation, but could turn into aggregation, RestrictedQuery[?] means none present and none addable
 */
class RestrictedQuery[A, C <: ResultCategory](using ResultTag[A])(protected val wrapped: Query[A, C]) extends DatabaseAST[A]:
  val tag: ResultTag[A] = qTag
  def toQuery: Query[A, C] = wrapped

  @targetName("restrictedQueryFlatMap")
  def flatMap[B: ResultTag](f: Expr.Ref[A, NonScalarExpr] => Query[B, ?]): RestrictedQuery[B, BagResult] = RestrictedQuery(wrapped.flatMap(f))
  @targetName("restrictedQueryFlatMapRestricted")
  def flatMap[B: ResultTag](f: Expr.Ref[A, NonScalarExpr] => RestrictedQuery[B, ?]): RestrictedQuery[B, BagResult] =
    val toR: Expr.Ref[A, NonScalarExpr] => Query[B, ?] = arg => f(arg).toQuery
    RestrictedQuery(wrapped.flatMap(toR))
  def map[B: ResultTag](f: Expr.Ref[A, NonScalarExpr] => Expr[B, NonScalarExpr]): RestrictedQuery[B, BagResult] = RestrictedQuery(wrapped.map(f))
  def map[B <: AnyNamedTuple : Expr.IsTupleOfExpr](using ResultTag[NamedTuple.Map[B, Expr.StripExpr]])(f: Expr.Ref[A, NonScalarExpr] => B): RestrictedQuery[NamedTuple.Map[B, Expr.StripExpr], BagResult] = RestrictedQuery(wrapped.map(f))
  def withFilter(p: Expr.Ref[A, NonScalarExpr] => Expr[Boolean, NonScalarExpr]): RestrictedQuery[A, C] = RestrictedQuery(wrapped.withFilter(p))
  def filter(p: Expr.Ref[A, NonScalarExpr] => Expr[Boolean, NonScalarExpr]): RestrictedQuery[A, C] = RestrictedQuery(wrapped.filter(p))

  def distinct: RestrictedQuery[A, SetResult] = RestrictedQuery(wrapped.distinct)

  def union(that: RestrictedQuery[A, ?]): RestrictedQuery[A, SetResult] =
    RestrictedQuery(Query.Union(wrapped, that.toQuery))

  def unionAll(that: RestrictedQuery[A, ?]): RestrictedQuery[A, BagResult] =
    RestrictedQuery(Query.UnionAll(wrapped, that.toQuery))

  @targetName("unionQuery")
  def union(that: Query[A, ?]): RestrictedQuery[A, SetResult] =
    RestrictedQuery(Query.Union(wrapped, that))
  @targetName("unionAllQuery")
  def unionAll(that: Query[A, ?]): RestrictedQuery[A, BagResult] =
    RestrictedQuery(Query.UnionAll(wrapped, that))

  // TODO: Does nonEmpty count as non-monotone?
  def nonEmpty: Expr[Boolean, NonScalarExpr] =
    Expr.NonEmpty(wrapped)

  def isEmpty: Expr[Boolean, NonScalarExpr] =
    Expr.IsEmpty(wrapped)


