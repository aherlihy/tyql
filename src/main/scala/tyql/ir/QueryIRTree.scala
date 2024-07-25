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

  def generateFullQuery(ast: DatabaseAST[?], symbols: Map[String, RelationOp]): (QueryIRNode & TopLevel) =
    val result = generateQuery(ast, symbols)
    result.flags = result.flags + SelectFlags.TopLevel // ignore top-level parens
    result

  var idCount = 0
  /**
   * Convert table.filter(p1).filter(p2) => table.filter(p1 && p2).
   * Example of a heuristic tree transformation/optimization
   */
  private def collapseFilters(filters: Seq[Expr.Fun[?, ?]], comprehension: DatabaseAST[?], symbols: Map[String, RelationOp]): (Seq[Expr.Fun[?, ?]], RelationOp) =
    comprehension match
      case table: Table[?] =>
        (filters, TableLeaf(table.$name, table))
      case filter: Query.Filter[?] =>
        collapseFilters(filters :+ filter.$pred, filter.$from, symbols)
      case _ => (filters, generateQuery(comprehension, symbols))

  private def collapseFlatMap(sources: Seq[RelationOp], symbols: Map[String, RelationOp], body: DatabaseAST[?] | Expr[?]): (Seq[RelationOp], QueryIRNode) =
    body match
      case map: Query.Map[?, ?] =>
        val srcIR = generateQuery(map.$from, symbols)
        val bodyIR = generateFun(map.$query, srcIR, symbols)
        (sources :+ srcIR, bodyIR)
      case flatMap: Query.FlatMap[?, ?] =>
        val srcIR = generateQuery(flatMap.$from, symbols)
        val bodyAST = flatMap.$query
        collapseFlatMap(
          sources :+ srcIR,
          symbols + (bodyAST.$param.stringRef() -> srcIR),
          bodyAST.$body
        )
      case aggFlatMap: Aggregation.AggFlatMap[?, ?] => // TODO: merge into flatMap
        val srcIR = generateQuery(aggFlatMap.$from, symbols)
        val outerBodyAST = aggFlatMap.$query
        outerBodyAST.$body match
          case recur: (Query.Map[?, ?] | Query.FlatMap[?, ?] | Aggregation.AggFlatMap[?, ?]) =>
            collapseFlatMap(
              sources :+ srcIR,
              symbols + (outerBodyAST.$param.stringRef() -> srcIR),
              outerBodyAST.$body
            )
          case _ => // base case
            val innerBodyIR = generateFun(outerBodyAST, srcIR, symbols)
            (sources :+ srcIR, innerBodyIR)
      case _ => throw Exception(s"Unimplemented: collapsing flatMap on type $body")

  // TODO: probably should parametrize collapse so it works with different nodes
  private def collapseSort(sorts: Seq[(Expr.Fun[?, ?], Ord)], comprehension: DatabaseAST[?], symbols: Map[String, RelationOp]): (Seq[(Expr.Fun[?, ?], Ord)], RelationOp) =
    comprehension match
      case table: Table[?] =>
        (sorts.reverse, TableLeaf(table.$name, table)) // reverse because order matters, unlike filters
      case sort: Query.Sort[?, ?] =>
        collapseSort(sorts :+ (sort.$body, sort.$ord), sort.$from, symbols)
      case _ => (sorts, generateQuery(comprehension, symbols))

  /**
   * Generate top-level or subquery
   *
   * @param ast
   * @return
   */
  private def generateQuery(ast: DatabaseAST[?], symbols: Map[String, RelationOp]): RelationOp =
    println(s"genQuery: ast=$ast")
    ast match
      case table: Table[?] =>
        TableLeaf(table.$name, table)
      case map: Query.Map[?, ?] =>
        val fromNode = generateQuery(map.$from, symbols)
        val attrNode = generateFun(map.$query, fromNode, symbols)
        fromNode.appendProject(attrNode, map)
      case filter: Query.Filter[?] =>
        val (predicateASTs, tableIR) = collapseFilters(Seq(), filter, symbols)
        val predicateExprs = predicateASTs.map(pred =>
          generateFun(pred, tableIR, symbols)
        )
        val where = WhereClause(predicateExprs, filter.$pred.$body)
        tableIR match
          case s: SelectQuery if s.project.isInstanceOf[SelectAllExpr] =>
            tableIR.appendWhere(Seq(where), filter)
          case t: TableLeaf =>
            tableIR.appendWhere(Seq(where), filter)
          case _ => // cannot unnest because source had projection, sort, etc.
            SelectQuery(SelectAllExpr(), Seq(tableIR), Seq(where), None, filter)
      case flatMap: (Query.FlatMap[?, ?] | Aggregation.AggFlatMap[?, ?]) =>
        val (tableIRs, projectIR) = collapseFlatMap(Seq(), Map(), flatMap)
//        println(s"in flatMap, tableIRs=$tableIRs, projectIR=$projectIR")
        // TODO: this is where could create more complex join nodes,
        //  for now just r1.filter(f1).flatMap(a1 => r2.filter(f2).map(a2 => body(a1, a2))) => SELECT body FROM a1, a2 WHERE f1 AND f2
        if tableIRs.length == 1 then
          SelectQuery(projectIR, tableIRs, Seq(), None, flatMap)
        else try
          tableIRs.reduce((q1, q2) =>
            q2 match
              case s: SelectQuery =>
                q1.appendSubquery(s, flatMap)
              case t: TableLeaf =>
                q1.appendSubquery(SelectQuery(SelectAllExpr(), Seq(t), Seq(), None, t.ast), flatMap)
              case _ => throw new Exception(s"Cannot unnest query")
          ).appendProject(projectIR, flatMap)
        catch
          case e: Exception => SelectQuery(projectIR, tableIRs, Seq(), None, flatMap)

      case union: Query.Union[?] =>
        val lhs = generateQuery(union.$this, symbols)
        val rhs = generateQuery(union.$other, symbols)
        val op = if union.$dedup then "UNION" else "UNION ALL"
        BinRelationOp(lhs, rhs, op, union)
      case intersect: Query.Intersect[?] =>
        val lhs = generateQuery(intersect.$this, symbols)
        val rhs = generateQuery(intersect.$other, symbols)
        BinRelationOp(lhs, rhs, "INTERSECT", intersect)
      case sort: Query.Sort[?, ?] =>
        val (orderByASTs, tableIR) = collapseSort(Seq(), sort, symbols)
        val orderByExprs = orderByASTs.map(ord =>
          (generateFun(ord._1, tableIR, symbols), ord._2)
        )
        OrderedQuery(tableIR, orderByExprs, sort)
      case limit: Query.Limit[?] =>
        val from = generateQuery(limit.$from, symbols)
        BinRelationOp(from, Literal(limit.$limit.toString, limit.$limit), "LIMIT", limit)
      case offset: Query.Offset[?] =>
        val from = generateQuery(offset.$from, symbols)
        BinRelationOp(from, Literal(offset.$offset.toString, offset.$offset), "OFFSET", offset)
      case distinct: Query.Distinct[?] =>
        val query = generateQuery(distinct.$from, symbols)
        query.flags = query.flags + SelectFlags.Distinct
        query
      case _ => throw new Exception(s"Unimplemented Relation-Op AST: $ast")

  private def generateFun(fun: Expr.Fun[?, ?], appliedTo: QueryIRAliasable, symbols: Map[String, QueryIRAliasable]): QueryIRNode =
    fun.$body match
      case r: Expr.Ref[?] if r.stringRef() == fun.$param.stringRef() => SelectAllExpr() // special case identity function
      case e: Expr[?] => generateExpr(e, symbols + (fun.$param.stringRef() -> appliedTo))
      case _ => ??? // TODO: find better way to differentiate

  private def generateExpr(ast: Expr[?], symbols: Map[String, QueryIRAliasable]): QueryIRNode =
    ast match
      case ref: Expr.Ref[?] =>
        val name = ref.stringRef()
        val sub = symbols(name)
        QueryIRVar(sub, name, ref) // TODO: singleton?
      case s: Expr.Select[?] => SelectExpr(s.$name, generateExpr(s.$x, symbols), s)
      case p: Expr.Project[?] =>
        val a = NamedTuple.toTuple(p.$a.asInstanceOf[NamedTuple[Tuple, Tuple]]) // TODO: bug? See https://github.com/scala/scala3/issues/21157
        val namedTupleNames = p.tag match
          case ResultTag.NamedTupleTag(names, types) => names.lift
          case _ => Seq()
        val children = a.toList.zipWithIndex
          .map((expr, idx) =>
            val e = expr.asInstanceOf[Expr[?]]
            AttrExpr(generateExpr(e, symbols), namedTupleNames(idx), e)
          )
        ProjectClause(children, p)
      case g: Expr.Gt => BinExprOp(generateExpr(g.$x, symbols), generateExpr(g.$y, symbols), ">", g)
      case a: Expr.And => BinExprOp(generateExpr(a.$x, symbols), generateExpr(a.$y, symbols), "AND", a)
      case a: Expr.Eq => BinExprOp(generateExpr(a.$x, symbols), generateExpr(a.$y, symbols), "=", a)
      case a: Expr.Concat[?, ?] => BinExprOp(generateExpr(a.$x, symbols), generateExpr(a.$y, symbols), ",", a)
      case l: Expr.IntLit => Literal(s"${l.$value}", l)
      case l: Expr.StringLit => Literal(s"\"${l.$value}\"", l)
      case a: Aggregation[?] => generateAggregation(a, symbols)
      case _ => throw new Exception(s"Unimplemented Expr AST: $ast")

  private def generateAggregation(ast: Aggregation[?], symbols: Map[String, QueryIRAliasable]): QueryIRNode =
    ast match
      case s: Aggregation.Sum[?] => UnaryExprOp(generateExpr(s.$a, symbols), o => s"SUM($o)", s)
      case s: Aggregation.Avg[?] => UnaryExprOp(generateExpr(s.$a, symbols), o => s"AVG($o)", s)
      case s: Aggregation.Min[?] => UnaryExprOp(generateExpr(s.$a, symbols), o => s"MIN($o)", s)
      case s: Aggregation.Max[?] => UnaryExprOp(generateExpr(s.$a, symbols), o => s"MAX($o)", s)
      case _ => throw new Exception(s"Unimplemented aggregation op: $ast")

trait RelationOp extends QueryIRAliasable with TopLevel:
  def appendWhere(w: Seq[QueryIRNode], astOther: DatabaseAST[?]): RelationOp
  def appendProject(p: QueryIRNode, astOther: DatabaseAST[?]): RelationOp
  def appendSubquery(q: SelectQuery, astOther: DatabaseAST[?]): RelationOp

case class TableLeaf(tableName: String, ast: Table[?]) extends RelationOp with QueryIRLeaf:
  val name = s"$tableName${QueryIRTree.idCount}"
  QueryIRTree.idCount += 1
  override def alias = name
  override def toSQLString(): String = s"$tableName as $name"

  override def toString: String = s"TableLeaf($tableName as $name)"

  override def appendWhere(w: Seq[QueryIRNode], astOther: DatabaseAST[?]): RelationOp =
    SelectQuery(SelectAllExpr(), Seq(this), w, Some(alias), astOther)

  override def appendProject(p: QueryIRNode, astOther: DatabaseAST[?]): RelationOp =
    SelectQuery(p, Seq(this), Seq(), None, astOther)


  override def appendSubquery(q: SelectQuery, astOther: DatabaseAST[?]): RelationOp =
    SelectQuery(
      q.project,
      this +: q.from,
      q.where,
      None, //Some(q.alias),
      astOther
    )

/**
 * Select: SELECT <ProjectClause> FROM <QueryIRSource> WHERE <WhereClause>
 */
case class SelectQuery(project: QueryIRNode,
                       from: Seq[QueryIRAliasable],
                       where: Seq[QueryIRNode],
                       overrideAlias: Option[String],
                       ast: DatabaseAST[?]) extends RelationOp:
  val children = project +: (from ++ where)
  val name = overrideAlias.getOrElse({
    val latestVar = s"subquery${QueryIRTree.idCount}"
    QueryIRTree.idCount += 1
    latestVar
  })
  override def alias = name
  override def appendWhere(w: Seq[QueryIRNode], astOther: DatabaseAST[?]): RelationOp =
    SelectQuery(project, from, where ++ w, Some(alias), astOther)

  // TODO: define semantics of map(f1).map(f2), could collapse into map(f2(f1))?
  override def appendProject(p: QueryIRNode, astOther: DatabaseAST[?]): RelationOp =
    project match
      case s: SelectAllExpr => SelectQuery(p, from, where, overrideAlias, ast)
      case _ => // Could collapse, for now use subquery
        SelectQuery(p, Seq(this), Seq(), None, astOther)

  /**
   * Merge with another select query to avoid extra nesting
   */
  override def appendSubquery(q: SelectQuery, astOther: DatabaseAST[?]): RelationOp =
    val newP = if (project.isInstanceOf[SelectAllExpr]) // TODO: differentiate between * and no map?
      q.project
    else if (q.project.isInstanceOf[SelectAllExpr])
      project
    else
      // TODO: define semantics of map(f1).map(f2), could collapse into map(f2(f1))?
      throw new Exception("Unimplemented: merging two subqueries with project stmts")

    val newF = from ++ q.from
    val newW = where ++ q.where
    SelectQuery(newP, newF, newW, None, astOther) // TODO: alias?

  override def toSQLString(): String =
    val (open, close) = if flags.contains(SelectFlags.TopLevel) then ("", "") else ("(", ")")
    val aliasStr = if flags.contains(SelectFlags.TopLevel) then "" else s" as $alias"
    val flagsStr = if flags.contains(SelectFlags.Distinct) then "DISTINCT " else ""
    val projectStr = project.toSQLString()
    val fromStr = from.map(f => f.toSQLString()).mkString("", ", ", "")
    val whereStr = if where.nonEmpty then
      s" WHERE ${if where.size == 1 then where.head.toSQLString() else where.map(f => f.toSQLString()).mkString("(", " AND ", ")")}" else
      ""
    s"${open}SELECT $flagsStr$projectStr FROM $fromStr$whereStr$close$aliasStr"

  override def toString: String = // for debugging
    s"SelectQuery(\n\talias=$alias,\n\tproject=$project,\n\tfrom=$from,\n\twhere=$where\n)"

case class OrderedQuery(query: RelationOp, sortFn: Seq[(QueryIRNode, Ord)], ast: DatabaseAST[?]) extends RelationOp:
  query.flags = query.flags + SelectFlags.TopLevel
  override val children: Seq[QueryIRNode] = query +: sortFn.map(_._1)
  val latestVar = s"subquery${QueryIRTree.idCount}"
  QueryIRTree.idCount += 1
  override def alias = latestVar

  val orders: Seq[Ord] = sortFn.map(_._2)

  override def appendWhere(w: Seq[QueryIRNode], astOther: DatabaseAST[?]): RelationOp =
    // Does not trigger subquery, e.g. relation.sort(s).filter(f) => SELECT * FROM relation WHERE f ORDER BY s
    OrderedQuery(query.appendWhere(w, astOther), sortFn, ast)

  override def appendProject(p: QueryIRNode, astOther: DatabaseAST[?]): RelationOp =
    // Triggers a subquery, e.g. relation.sort(s).map(m) => SELECT m FROM (SELECT * from relation ORDER BY s).
    // Note relation.map(m).sort(s) => SELECT m FROM relation ORDER BY s
    SelectQuery(
      p,
      Seq(this),
      Seq(),
      None,
      astOther
    )

  override def appendSubquery(q: SelectQuery, astOther: DatabaseAST[?]): RelationOp =
    // Triggers subquery
    SelectQuery(
      SelectAllExpr(),
      Seq(this),
      Seq(),
      None,
      astOther
    )

  override def toSQLString(): String =
    val (open, close) = if flags.contains(SelectFlags.TopLevel) then ("", "") else ("(", ")")
    val aliasStr = if flags.contains(SelectFlags.TopLevel) then "" else s" as $alias"
    s"$open${query.toSQLString()} ORDER BY ${sortFn.map(s => s"${s._1.toSQLString()} ${s._2.toString}").mkString("", ", ", "")}$close$aliasStr"

case class BinRelationOp(lhs: QueryIRNode, rhs: QueryIRNode, op: String, ast: Query[?]) extends RelationOp:
  override val children: Seq[QueryIRNode] = Seq(lhs, rhs)
  val latestVar = s"subquery${QueryIRTree.idCount}"
  QueryIRTree.idCount += 1
  override def alias = latestVar

  override def appendWhere(w: Seq[QueryIRNode], astOther: DatabaseAST[?]): RelationOp =
    SelectQuery(
      SelectAllExpr(),
      Seq(this),
      w,
      None,
      astOther
    )

  override def appendProject(p: QueryIRNode, astOther: DatabaseAST[?]): RelationOp =
    SelectQuery(
      p,
      Seq(this),
      Seq(),
      None,
      astOther
    )

  override def appendSubquery(q: SelectQuery, astOther: DatabaseAST[?]): RelationOp =
    SelectQuery(
      SelectAllExpr(),
      Seq(this),
      Seq(),
      None,
      astOther
    )

  override def toSQLString(): String =
    val (open, close) = if flags.contains(SelectFlags.TopLevel) then ("", "") else ("(", ")")
    val aliasStr = if flags.contains(SelectFlags.TopLevel) then "" else s" as $alias"
    s"${lhs.toSQLString()} $op ${rhs.toSQLString()}"

// Single WHERE clause containing 1+ predicates
case class WhereClause(children: Seq[QueryIRNode], ast: Expr[?]) extends QueryIRNode:
  override def toSQLString(): String = if children.size == 1 then children.head.toSQLString() else  s"${children.map(_.toSQLString()).mkString("", " AND ", "")}"

case class PredicateExpr(child: QueryIRNode, ast: Expr.Fun[?, ?]) extends QueryIRNode:
  override val children: Seq[QueryIRNode] = Seq(child)
  override def toSQLString(): String = ???

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

case class QueryIRVar(toSub: QueryIRAliasable, name: String, ast: Expr.Ref[?]) extends QueryIRLeaf:
  override def toSQLString() = toSub.alias

  override def toString: String = s"VAR(${toSub.alias}.$name)"

// TODO: can't assume stringRep is universal, need to specialize for DB backend
case class Literal(stringRep: String, ast: Expr[?]) extends QueryIRLeaf:
  override def toSQLString(): String = stringRep

case class EmptyLeaf(ast: DatabaseAST[?] = null) extends QueryIRLeaf:
  override def toSQLString(): String = ""

// Helper to print AST subtree
case class PlaceHolderNode(ast: DatabaseAST[?] | Expr[?]) extends QueryIRLeaf with QueryIRAliasable:
  override def alias: String = "placeholder"
  override def toSQLString(): String = s"$ast"