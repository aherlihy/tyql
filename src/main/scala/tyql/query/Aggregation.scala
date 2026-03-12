package tyql

import tyql.Expr.{Fun, Ref}

type ToRef[TT <: Tuple] = Tuple.Map[TT, Ref]

trait Aggregation[
  AllSourceTypes <: Tuple,
  Result
](using ResultTag[Result]) extends DatabaseAST[Result] with Expr[Result]:
  def groupBySource[GroupResult]
    (groupingFn: ToRef[AllSourceTypes] => Expr[GroupResult])
  : Query.NewGroupBy[AllSourceTypes, Result, GroupResult]

object Aggregation:

  def getSourceTables[T](q: Query[T], sources: Seq[(String, ResultTag[?])]): Seq[(String, ResultTag[?])] =
    q match
      case t: Table[?] => (sources :+ (s"${t.$name}", t.tag))
      case f: Query.Filter[?] => getSourceTables(f.$from, sources)
      case m: Query.Map[?, ?] => getSourceTables(m.$from, sources)
      case fm: Query.FlatMap[?, ?] =>
        val srcOuter = getSourceTables(fm.$from, sources)
        val srcInner = getSourceTables(fm.$query.$body, srcOuter)
        srcInner
      case qr: Query.QueryRef[?] => (sources :+ (s"${qr.stringRef()}", qr.tag))
      case _ =>
        throw new Exception(s"GroupBy on result of ${q} not yet implemented")

  def getNestedSourceTables(q: Fun[?, ?], sources: Seq[(String, ResultTag[?])]): Seq[(String, ResultTag[?])] =
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
  ]($from: Query[Tuple.Head[AllSourceTypes]],
    $query: Expr.Fun[Tuple.Head[AllSourceTypes], Expr[B]]) extends Aggregation[AllSourceTypes, B]:

    def groupBySource[GroupResult]
      (groupingFn: ToRef[AllSourceTypes] => Expr[GroupResult])
    : Query.NewGroupBy[AllSourceTypes, B, GroupResult] =

      val sourceTagsOuter = getSourceTables($from, Seq.empty)
      val sourceTags = getNestedSourceTables($query, sourceTagsOuter)

      val argRefs = sourceTags.map(_._2).zipWithIndex.map((tag, idx) => Ref(idx)(using tag)).toArray
      val refsTuple = Tuple.fromArray(argRefs).asInstanceOf[ToRef[AllSourceTypes]]

      val groupResult = groupingFn(refsTuple)

      Query.NewGroupBy(this, groupResult, argRefs, sourceTags, None)

  case class AggFilter[A: ResultTag]($from: Query[A], $pred: Expr.Fun[A, Expr[Boolean]]) extends Aggregation[A *: EmptyTuple, A]:
    def groupBySource[GroupResult]
      (groupingFn: ToRef[A *: EmptyTuple] => Expr[GroupResult])
    : Query.NewGroupBy[A *: EmptyTuple, A, GroupResult] =
      ???
