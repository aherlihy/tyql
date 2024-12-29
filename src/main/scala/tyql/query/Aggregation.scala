package tyql

import tyql.Expr.{Fun, Ref}

type ToNonScalarRef[TT <: Tuple] = Tuple.Map[TT, [T] =>> Ref[T, NonScalarExpr]]

trait Aggregation[
  AllSourceTypes <: Tuple,
  Result
](using ResultTag[Result]) extends DatabaseAST[Result] with Expr[Result, NonScalarExpr]:
  def groupBySource[GroupResult, GroupShape <: ExprShape]
    (groupingFn: ToNonScalarRef[AllSourceTypes] => Expr[GroupResult, GroupShape])
  : Query.NewGroupBy[AllSourceTypes, Result, GroupResult, GroupShape]

object Aggregation:

  def getSourceTables[T](q: Query[T, ?], sources: Seq[(String, ResultTag[?])]): Seq[(String, ResultTag[?])] =
    q match
      case t: Table[?] => (sources :+ (s"${t.$name}", t.tag))
      case f: Query.Filter[?, ?] => getSourceTables(f.$from, sources)
      case m: Query.Map[?, ?] => getSourceTables(m.$from, sources)
      case fm: Query.FlatMap[?, ?] =>
        val srcOuter = getSourceTables(fm.$from, sources)
        val srcInner = getSourceTables(fm.$query.$body, srcOuter)
        srcInner
      case qr: Query.QueryRef[?, ?] => (sources :+ (s"${qr.stringRef()}", qr.tag))
      case mr: Query.MultiRecursive[?] => getSourceTables(mr.$resultQuery, sources)//sources ++ mr.$param.map(rqr => (s"${rqr.toQueryRef.stringRef()}", rqr.tag)))
      case _ =>
        throw new Exception(s"GroupBy on result of ${q} not yet implemented")

  def getNestedSourceTables(q: Fun[?, ?, ?], sources: Seq[(String, ResultTag[?])]): Seq[(String, ResultTag[?])] =
    q.$body match
      case AggFlatMap(src, q) =>
        val srcOuter = getSourceTables(src, sources)
        val srcInner = getNestedSourceTables(q, srcOuter)
        srcInner
      case AggFilter(from, p) => getSourceTables(from, sources)
      case _ => sources

  case class AggFlatMap[
    AllSourceTypes <: Tuple,
    B: ResultTag
  ]($from: Query[Tuple.Head[AllSourceTypes], ?],
    $query: Expr.Fun[Tuple.Head[AllSourceTypes], Expr[B, ?], ?]) extends Aggregation[AllSourceTypes, B]:

    def groupBySource[GroupResult, GroupShape <: ExprShape]
      (groupingFn: ToNonScalarRef[AllSourceTypes] => Expr[GroupResult, GroupShape])
    : Query.NewGroupBy[AllSourceTypes, B, GroupResult, GroupShape] =

      val sourceTagsOuter = getSourceTables($from, Seq.empty)
      val sourceTags = getNestedSourceTables($query, sourceTagsOuter)

      val argRefs = sourceTags.map(_._2).zipWithIndex.map((tag, idx) => Ref(idx)(using tag)).toArray
      val refsTuple = Tuple.fromArray(argRefs).asInstanceOf[ToNonScalarRef[AllSourceTypes]]

      val groupResult = groupingFn(refsTuple)

      Query.NewGroupBy(this, groupResult, argRefs.toIndexedSeq, sourceTags, None)

  case class AggFilter[A: ResultTag]($from: Query[A, ?], $pred: Expr.Pred[A, ScalarExpr]) extends Aggregation[A *: EmptyTuple, A]:
    def groupBySource[GroupResult, GroupShape <: ExprShape]
      (groupingFn: ToNonScalarRef[A *: EmptyTuple] => Expr[GroupResult, GroupShape])
    : Query.NewGroupBy[A *: EmptyTuple, A, GroupResult, GroupShape] =
      ???
