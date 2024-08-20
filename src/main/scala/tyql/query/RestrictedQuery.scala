package tyql

import tyql.{DatabaseAST, Expr, NExpr, Query, ResultTag}

import scala.NamedTuple.AnyNamedTuple
import scala.annotation.targetName

/**
 * A restricted reference to a query that disallows aggregation.
 * Explicitly do not export aggregate, or any aggregation helpers, exists, etc.
 *
 * Methods can accept RestrictedQuery[A] or Query[A]
 * NOTE: Query[?] indicates no aggregation, but could turn into aggregation, RestrictedQuery[?] means none present and none addable
 */
class RestrictedQuery[A](using ResultTag[A])(protected val wrapped: Query[A]) extends DatabaseAST[A]:
  def toQuery: Query[A] = wrapped

  @targetName("restrictedQueryFlatMap")
  def flatMap[B: ResultTag](f: Expr.Ref[A, NExpr] => Query[B]): RestrictedQuery[B] = RestrictedQuery(wrapped.flatMap(f))
  @targetName("restrictedQueryFlatMapRestricted")
  def flatMap[B: ResultTag](f: Expr.Ref[A, NExpr] => RestrictedQuery[B]): RestrictedQuery[B] =
    val toR: Expr.Ref[A, NExpr] => Query[B] = arg => f(arg).toQuery
    RestrictedQuery(wrapped.flatMap(toR))
  def map[B: ResultTag](f: Expr.Ref[A, NExpr] => Expr[B, NExpr]): RestrictedQuery[B] = RestrictedQuery(wrapped.map(f))
  def map[B <: AnyNamedTuple : Expr.IsTupleOfExpr](using ResultTag[NamedTuple.Map[B, Expr.StripExpr]])(f: Expr.Ref[A, NExpr] => B): RestrictedQuery[NamedTuple.Map[B, Expr.StripExpr]] = RestrictedQuery(wrapped.map(f))
  def withFilter(p: Expr.Ref[A, NExpr] => Expr[Boolean, NExpr]): RestrictedQuery[A] = RestrictedQuery(wrapped.withFilter(p))
  def filter(p: Expr.Ref[A, NExpr] => Expr[Boolean, NExpr]): RestrictedQuery[A] = RestrictedQuery(wrapped.filter(p))
  def union(that: RestrictedQuery[A]): RestrictedQuery[A] =
    RestrictedQuery(Query.Union(wrapped, that.toQuery, true))

  def unionAll(that: RestrictedQuery[A]): RestrictedQuery[A] =
    RestrictedQuery(Query.Union(wrapped, that.toQuery, false))

  @targetName("unionQuery")
  def union(that: Query[A]): RestrictedQuery[A] =
    RestrictedQuery(Query.Union(wrapped, that, true))
  @targetName("unionAllQuery")
  def unionAll(that: Query[A]): RestrictedQuery[A] =
    RestrictedQuery(Query.Union(wrapped, that, false))


