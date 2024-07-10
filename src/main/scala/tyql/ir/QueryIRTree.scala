package tyql

import language.experimental.namedTuples
import NamedTuple.{NamedTuple, AnyNamedTuple}
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

object QueryIRTree:

  def generateTopQuery(ast: DatabaseAST[?]): SelectQuery =
    val generated = generateQuery(ast)
    val result = generated match
      case f: FromClause => // if we have no project then fill in select *
        SelectQuery(
          SelectAllExpr(),
          generated,
          ast
        )
      case s: SelectQuery =>
        s
      case _ => throw Exception("Malformed AST: Non-top level query returned from generate query")
    result.topLevel = true // ignore top-level parens
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
        SelectQuery(generateFun(map.$query, fromNode), fromNode, map)
      case filter: Query.Filter[?] =>
        val (predicateASTs, tableIR) = collapseFilters(Seq(), filter)
        val predicateExprs = predicateASTs.map(pred =>
          generateFun(pred, tableIR)
        )
        val where = WhereClause(predicateExprs, filter)
        FromClause(tableIR, where, filter)
      case _ => ??? // either flatMap or aggregate, TODO

  private def generateFun(fun: Expr.Fun[?, ?], appliedTo: QueryIRAliasable): QueryIRNode =
    fun.$f match
      case e: Expr[?] => generateExpr(e, Map((fun.$param.$name, appliedTo)))
      case _ => ??? // TODO: find better way to differentiate

  private def generateExpr(ast: Expr[?], symbols: Map[String, QueryIRAliasable]): QueryIRNode =
    ast match
      case ref: Expr.Ref[?] =>
        val sub = symbols(ref.$name) // TODO: pass symbols down tree?
        QueryIRVar(sub, ref.$name, ref) // TODO: singleton?
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
      case g: Expr.Gt => BinOp(generateExpr(g.$x, symbols), generateExpr(g.$y, symbols), ">", g)
      case l: Expr.IntLit => Literal(s"${l.$value}", l)
      case _ => PlaceHolderNode(ast)

/**
 * Select: SELECT <ProjectClause> FROM <QueryIRSource> WHERE <WhereClause>
 */
case class SelectQuery(project: QueryIRNode,
                       from: QueryIRAliasable,
                       ast: DatabaseAST[?]) extends QueryIRAliasable:
  var topLevel = false
  val children = Seq(project, from)
  val latestVar = s"subquery${QueryIRTree.idCount}"
  QueryIRTree.idCount += 1
  override def alias = latestVar

  override def toSQLString(): String =
    val (open, close) = if topLevel then ("", "") else ("(", ")")
    val aliasStr = if topLevel then "" else s" as $alias"
    s"${open}SELECT ${project.toSQLString()} FROM ${from.toSQLString()}$close$aliasStr"

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

// TODO: can't assume op is universal, need to specialize for DB backend
case class BinOp(lhs: QueryIRNode, rhs: QueryIRNode, op: String, ast: Expr[?]) extends QueryIRNode:
  override val children: Seq[QueryIRNode] = Seq(lhs, rhs)
  override def toSQLString(): String = s"${lhs.toSQLString()} $op ${rhs.toSQLString()}"

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

case class PlaceHolderNode(ast: DatabaseAST[?] | Expr[?]) extends QueryIRLeaf:
  override def toSQLString(): String = s"$ast"