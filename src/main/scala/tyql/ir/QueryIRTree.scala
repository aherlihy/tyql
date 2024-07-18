package tyql

import language.experimental.namedTuples
import NamedTuple.{AnyNamedTuple, NamedTuple}
import NamedTupleDecomposition.*
/**
 * Logical query plan tree
 * Moves all type parameters into terms
 */
trait QueryIRNode:
  val ast: DatabaseAST[?] | Expr[?] | Expr.Fun[?, ?] // Best-effort, keep AST around for debugging, TODO: probably remove, or replace only with ResultTag
  val children: Seq[QueryIRNode]
  def toSQLString(): String

trait QueryIRLeaf extends QueryIRNode:
  override val children: Seq[QueryIRNode] = Seq()

trait QueryIRAliasable extends QueryIRNode:
  def alias: String

enum SelectFlags:
  case Distinct
  case TopLevel

trait TopLevel:
  var flags: Set[SelectFlags] = Set.empty

object QueryIRTree:

  def generateFullQuery(ast: DatabaseAST[?]): (QueryIRNode & TopLevel) =
    val generated = generateQuery(ast)
    val result = generated match
      case f: (FromClause | OrderedFrom) => // if we have no project then fill in select *
        SelectQuery(
          SelectAllExpr(),
          Seq(generated),
          ast
        )
      case s: (SelectQuery | BinRelationOp) =>
        s
      case _ => throw Exception("Malformed AST: Non-top level query returned from generate query")
    result.flags = result.flags + SelectFlags.TopLevel // ignore top-level parens
    result

  var idCount = 0
  /**
   * Convert table.filter(p1).filter(p2) => table.filter(p1 && p2).
   * Example of a heuristic tree transformation/optimization
   */
  private def collapseFilters(filters: Seq[Expr.Fun[?, ?]], comprehension: DatabaseAST[?]): (Seq[Expr.Fun[?, ?]], QueryIRAliasable) =
    comprehension match
      case table: Table[?] =>
        (filters, TableLeaf(table.$name, table))
      case filter: Query.Filter[?] =>
        collapseFilters(filters :+ filter.$pred, filter.$from)
      case _ => (filters, generateQuery(comprehension))

  private def collapseFlatMap(sources: Seq[QueryIRAliasable], symbols: Map[String, QueryIRAliasable], body: DatabaseAST[?] | Expr[?]): (Seq[QueryIRAliasable], QueryIRNode) =
    body match
      case map: Query.Map[?, ?] =>
        val innerSrc = generateQuery(map.$from)
        val innerFnIR = generateFun(map.$query, innerSrc, symbols)
        (sources :+ innerSrc, innerFnIR)
      case flatMap: Query.FlatMap[?, ?] =>
        val outerSrc = generateQuery(flatMap.$from)
        val outerFnAST = flatMap.$query
        collapseFlatMap(
          sources :+ outerSrc,
          symbols + (outerFnAST.$param.stringRef() -> outerSrc),
          outerFnAST.$body
        )
      case aggFlatMap: Aggregation.AggFlatMap[?, ?] =>
        val outerSrc = generateQuery(aggFlatMap.$from)
        val outerFnAST = aggFlatMap.$query
        outerFnAST.$body match
          case recur: (Query.Map[?, ?] | Query.FlatMap[?, ?] | Aggregation.AggFlatMap[?, ?]) =>
            collapseFlatMap(
              sources :+ outerSrc,
              symbols + (outerFnAST.$param.stringRef() -> outerSrc),
              outerFnAST.$body
            )
          case _ => // base case
            val innerFnIR = generateFun(outerFnAST, outerSrc, symbols)
            (sources :+ outerSrc, innerFnIR)
//      case filter: Query.Filter[?] =>
//        collapseFilters(filters :+ filter.$pred, filter.$from)
      case _ => println(s"got $body"); ???

  // TODO: probably should parametrize collapse so it works with different nodes
  private def collapseSort(sorts: Seq[(Expr.Fun[?, ?], Ord)], comprehension: DatabaseAST[?]): (Seq[(Expr.Fun[?, ?], Ord)], QueryIRAliasable) =
    comprehension match
      case table: Table[?] =>
        (sorts.reverse, TableLeaf(table.$name, table)) // reverse because order matters, unlike filters
      case sort: Query.Sort[?, ?] =>
        collapseSort(sorts :+ (sort.$body, sort.$ord), sort.$from)
      case _ => (sorts, generateQuery(comprehension))
  /**
   * Generate top-level or subquery
   *
   * @param ast
   * @return
   */
  private def generateQuery(ast: DatabaseAST[?]): QueryIRAliasable =
    ast match
      case table: Table[?] =>
        FromClause(TableLeaf(table.$name, table), EmptyLeaf(), table)
      case map: Query.Map[?, ?] =>
        val fromNode = generateQuery(map.$from)
        val attrNode = generateFun(map.$query, fromNode)
        SelectQuery(attrNode, Seq(fromNode), map)
      case filter: Query.Filter[?] =>
        val (predicateASTs, tableIR) = collapseFilters(Seq(), filter)
        val predicateExprs = predicateASTs.map(pred =>
          generateFun(pred, tableIR)
        )
        val where = WhereClause(predicateExprs, filter)
        FromClause(tableIR, where, filter)
      case flatMap: Query.FlatMap[?, ?] =>
        val (tableIRs, projectIR) = collapseFlatMap(Seq(), Map(), flatMap)
        SelectQuery(projectIR, tableIRs, flatMap)
      case aggFlatMap: Aggregation.AggFlatMap[?, ?] =>
        val (tableIRs, projectIR) = collapseFlatMap(Seq(), Map(), aggFlatMap)
        SelectQuery(projectIR, tableIRs, aggFlatMap)
      case union: Query.Union[?] =>
        val lhs = generateFullQuery(union.$this)
        val rhs = generateFullQuery(union.$other)
        val op = if union.$dedup then "UNION" else "UNION ALL"
        BinRelationOp(lhs, rhs, op, union)
      case intersect: Query.Intersect[?] =>
        val lhs = generateFullQuery(intersect.$this)
        val rhs = generateFullQuery(intersect.$other)
        BinRelationOp(lhs, rhs, "INTERSECT", intersect)
      case sort: Query.Sort[?, ?] =>
        val (orderByASTs, tableIR) = collapseSort(Seq(), sort)
        println(s"tableIR = ${tableIR.toSQLString()}")
        val orderByExprs = orderByASTs.map(ord =>
          (generateFun(ord._1, tableIR), ord._2)
        )
        OrderedFrom(tableIR, orderByExprs, sort)
      case limit: Query.Limit[?] =>
        val from = generateFullQuery(limit.$from)
        BinRelationOp(from, Literal(limit.$limit.toString(), limit.$limit), "LIMIT", limit)
      case offset: Query.Offset[?] =>
        val from = generateFullQuery(offset.$from)
        BinRelationOp(from, Literal(offset.$offset.toString(), offset.$offset), "OFFSET", offset)
      case distinct: Query.Distinct[?] =>
        val query = generateFullQuery(distinct.$from)
        query.flags = query.flags + SelectFlags.Distinct
        query.asInstanceOf[QueryIRAliasable] // TODO: can we avoid this?

      case _ =>  println(s"Unimplemented Relation-Op AST: $ast"); ???

  private def generateFun(fun: Expr.Fun[?, ?], appliedTo: QueryIRAliasable, symbols: Map[String, QueryIRAliasable] = Map.empty): QueryIRNode =
    fun.$body match
      case r: Expr.Ref[?] if r.stringRef() == fun.$param.stringRef() => SelectAllExpr() // special case identity function
      case e: Expr[?] => generateExpr(e, symbols + (fun.$param.stringRef() -> appliedTo))
      case _ => ??? // TODO: find better way to differentiate

  private def generateExpr(ast: Expr[?], symbols: Map[String, QueryIRAliasable]): QueryIRNode =
    ast match
      case ref: Expr.Ref[?] =>
        val name = ref.stringRef()
        val sub = symbols(name) // TODO: pass symbols down tree?
        QueryIRVar(sub, name, ref) // TODO: singleton?
      case s: Expr.Select[?] => SelectExpr(s.$name, generateExpr(s.$x, symbols), s)
      case p: Expr.Project[?] =>
        val a = NamedTuple.toTuple(p.$a.asInstanceOf[NamedTuple[Tuple, Tuple]]) // TODO: bug? See https://github.com/scala/scala3/issues/21157
        val namedTupleNames = p.tag match
          case ResultTag.NamedTupleTag(names, types) => names.lift
          case _ => Seq()
        val children = a.toList.zipWithIndex
          .map((expr, idx) =>
            AttrExpr(generateExpr(expr.asInstanceOf[Expr[?]], symbols), namedTupleNames(idx), p)
          )
        ProjectClause(children, p)
      case g: Expr.Gt => BinExprOp(generateExpr(g.$x, symbols), generateExpr(g.$y, symbols), ">", g)
      case a: Expr.And => BinExprOp(generateExpr(a.$x, symbols), generateExpr(a.$y, symbols), "AND", a)
      case a: Expr.Eq => BinExprOp(generateExpr(a.$x, symbols), generateExpr(a.$y, symbols), "=", a)
      case a: Expr.Concat[?, ?] => BinExprOp(generateExpr(a.$x, symbols), generateExpr(a.$y, symbols), ",", a)
      case l: Expr.IntLit => Literal(s"${l.$value}", l)
      case a: Aggregation[?] => generateAggregation(a, symbols)
      case _ =>  println(s"Unimplemented Expr AST: $ast"); ???

  private def generateAggregation(ast: Aggregation[?], symbols: Map[String, QueryIRAliasable]): QueryIRNode =
    ast match
      case s: Aggregation.Sum[?] => UnaryExprOp(generateExpr(s.$a, symbols), o => s"SUM($o)", s)
      case s: Aggregation.Avg[?] => UnaryExprOp(generateExpr(s.$a, symbols), o => s"AVG($o)", s)
      case s: Aggregation.Min[?] => UnaryExprOp(generateExpr(s.$a, symbols), o => s"MIN($o)", s)
      case s: Aggregation.Max[?] => UnaryExprOp(generateExpr(s.$a, symbols), o => s"MAX($o)", s)
      case _ => ???


/**
 * Select: SELECT <ProjectClause> FROM <QueryIRSource> WHERE <WhereClause>
 */
case class SelectQuery(project: QueryIRNode,
                       from: Seq[QueryIRAliasable],
                       ast: DatabaseAST[?]) extends QueryIRAliasable with TopLevel:
  val children = project +: from // TODO: differentiate?
  val latestVar = s"subquery${QueryIRTree.idCount}"
  QueryIRTree.idCount += 1
  override def alias = latestVar

  override def toSQLString(): String =
    val (open, close) = if flags.contains(SelectFlags.TopLevel) then ("", "") else ("(", ")")
    val aliasStr = if flags.contains(SelectFlags.TopLevel) then "" else s" as $alias"
    val flagsStr = if flags.contains(SelectFlags.Distinct) then "DISTINCT " else ""
    s"${open}SELECT $flagsStr${project.toSQLString()} FROM ${from.map(f => f.toSQLString()).mkString("", ", ", "")}$close$aliasStr"

case class OrderedFrom(from: QueryIRAliasable, sortFn: Seq[(QueryIRNode, Ord)], ast: DatabaseAST[?]) extends QueryIRAliasable:
  override def alias: String = from.alias
  override val children: Seq[QueryIRNode] = from +: sortFn.map(_._1)
  val orders: Seq[Ord] = sortFn.map(_._2)
  override def toSQLString(): String =
    s"${from.toSQLString()} ORDER BY ${sortFn.map(s => s"${s._1.toSQLString()} ${s._2.toString}").mkString("", ", ", "")}"

// TODO: handle multiple sources with multiple aliases
case class FromClause(from: QueryIRAliasable, where: QueryIRNode, ast: DatabaseAST[?]) extends QueryIRAliasable:
  override def alias: String = from.alias
  override val children: Seq[QueryIRNode] = Seq(from, where)
  override def toSQLString(): String = s"${from.toSQLString()}${where.toSQLString()}"

case class WhereClause(children: Seq[QueryIRNode], ast: DatabaseAST[?]) extends QueryIRNode:
  override def toSQLString(): String = s" WHERE ${children.map(_.toSQLString()).mkString("", " AND ", "")}"

case class PredicateExpr(child: QueryIRNode, ast: Expr.Fun[?, ?]) extends QueryIRNode:
  override val children: Seq[QueryIRNode] = Seq(child)
  override def toSQLString(): String = ???

case class BinRelationOp(lhs: QueryIRNode, rhs: QueryIRNode, op: String, ast: Query[?]) extends QueryIRAliasable with TopLevel:
  override val children: Seq[QueryIRNode] = Seq(lhs, rhs)
  val latestVar = s"subquery${QueryIRTree.idCount}"
  QueryIRTree.idCount += 1
  override def alias = latestVar

  override def toSQLString(): String =
    val (open, close) = if flags.contains(SelectFlags.TopLevel) then ("", "") else ("(", ")")
    val aliasStr = if flags.contains(SelectFlags.TopLevel) then "" else s" as $alias"
    s"${lhs.toSQLString()} $op ${rhs.toSQLString()}"

// TODO: can't assume op is universal, need to specialize for DB backend
case class BinExprOp(lhs: QueryIRNode, rhs: QueryIRNode, op: String, ast: Expr[?]) extends QueryIRNode:
  override val children: Seq[QueryIRNode] = Seq(lhs, rhs)
  override def toSQLString(): String = s"${lhs.toSQLString()} $op ${rhs.toSQLString()}"

// TODO: can't assume op is universal, need to specialize for DB backend
case class UnaryExprOp(child: QueryIRNode, op: String => String, ast: Expr[?]) extends QueryIRNode:
  override val children: Seq[QueryIRNode] = Seq(child)
  override def toSQLString(): String = op(s"${child.toSQLString()}")

case class ProjectClause(children: Seq[QueryIRNode], ast: Expr[?]) extends QueryIRNode:
  override def toSQLString(): String = children.map(_.toSQLString()).mkString("", ", ", "")

// TODO: generate anonymous names, or allow generated queries to be unnamed, or only allow named tuple results?
// Note projected attributes with names is not the same as aliasing, and just exists for readability
case class AttrExpr(child: QueryIRNode, projectedName: Option[String], ast: Expr[?]) extends QueryIRNode:
  override val children: Seq[QueryIRNode] = Seq(child)
  val asStr = projectedName match
    case Some(value) => s" as $value"
    case None => ""
  override def toSQLString(): String = s"${child.toSQLString()}$asStr"

case class SelectExpr(attrName: String, from: QueryIRNode, ast: Expr[?]) extends QueryIRLeaf:
  override def toSQLString(): String = s"${from.toSQLString()}.$attrName"

case class SelectAllExpr() extends QueryIRLeaf:
  val ast = null
  override def toSQLString(): String = "*"

case class TableLeaf(tableName: String, ast: Table[?]) extends QueryIRAliasable with QueryIRLeaf:
  val name = s"$tableName${QueryIRTree.idCount}"
  QueryIRTree.idCount += 1
  override def alias = name
  override def toSQLString(): String = s"$tableName as $name"

case class QueryIRVar(toSub: QueryIRAliasable, name: String, ast: Expr.Ref[?]) extends QueryIRLeaf:
  override def toSQLString() = toSub.alias

// TODO: can't assume stringRep is universal, need to specialize for DB backend
case class Literal(stringRep: String, ast: Expr[?]) extends QueryIRLeaf:
  override def toSQLString(): String = stringRep

case class EmptyLeaf(ast: DatabaseAST[?] = null) extends QueryIRLeaf:
  override def toSQLString(): String = ""

// Helper to print AST subtree
case class PlaceHolderNode(ast: DatabaseAST[?] | Expr[?]) extends QueryIRLeaf with QueryIRAliasable:
  override def alias: String = "placeholder"
  override def toSQLString(): String = s"$ast"