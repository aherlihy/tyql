package tyql

import language.experimental.namedTuples
import NamedTuple.{NamedTuple, AnyNamedTuple}
import NamedTupleDecomposition.*
/**
 * Logical query plan tree
 * Moves all type parameters into terms
 */
trait QueryIRNode:
  val ast: DatabaseAST[?] | Expr[?] | Expr.Fun[?, ?] // Keep AST around for debugging, TODO: probably remove, or replace only with ResultTag
//  var parent: Option[QueryIRNode] = None // TODO: might not need backwards reference
  val children: Seq[QueryIRNode]
  def toSQLString(): String

trait QueryIRLeaf extends QueryIRNode:
  override val children: Seq[QueryIRNode] = Seq()

trait QueryIRSource extends QueryIRNode:
  def asStr: String

object QueryIRTree:
  var idCount = 0
  /**
   * Convert table.filter(p1).filter(p2) => table.filter(p1 && p2).
   * Example of a heuristic tree transformation/optimization
   */
  def collapseFilters(filters: Seq[Expr.Fun[?, ?]], comprehension: DatabaseAST[?]): (Seq[Expr.Fun[?, ?]], QueryIRSource) =
    comprehension match
      case table: Table[?] =>
        (filters, TableLeaf(table.$name, table))
      case filter: Query.Filter[?] =>
        collapseFilters(filters :+ filter.$pred, filter.$from)
      case _ => ??? // either error or subquery, TODO

  /**
   * Generate top-level or subquery
   * @param ast
   * @return
   */
  def generateQuery(ast: DatabaseAST[?]): QueryIRNode =
    ast match
      case map: Query.Map[?, ?] =>
        val (fromNode, whereNode) = map.$from match
          case table: Table[_] => // base case, FROM table
            (TableLeaf(table.$name, table), EmptyLeaf())
          case filter: Query.Filter[_] =>
            val (predicateASTs, tableIR) = collapseFilters(Seq(), filter)
            val predicateExprs = predicateASTs.map(pred =>
              generateFun(pred, tableIR)
            )
            (tableIR, WhereClause(predicateExprs, filter))
          case _ => ??? // either error or subquery, TODO

        val projectNode = generateFun(map.$query, fromNode)
        SelectQuery(projectNode, fromNode, whereNode, map)
      case _ => ??? // either flatMap or aggregate, TODO

  def generateFun(fun: Expr.Fun[?, ?], appliedTo: QueryIRSource): QueryIRNode =
    fun.$f match
      case e: Expr[?] => generateExpr(e, Map((fun.$param.$name, appliedTo)))
      case _ => ??? // TODO: find better way to differentiate

  def generateExpr(ast: Expr[?], symbols: Map[String, QueryIRSource]): QueryIRNode =
    ast match
      case ref: Expr.Ref[?] =>
        val sub = symbols(ref.$name) // TODO: pass symbols down tree?
        QueryIRVar(sub, ref.$name, ref) // TODO: singleton?
      case s: Expr.Select[?] => SelectExpr(s.$name, generateExpr(s.$x, symbols), s)
      case p: Expr.Project[?] =>
        val a = NamedTuple.toTuple(p.$a.asInstanceOf[NamedTuple[Tuple, Tuple]]) // TODO: bug?
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
                       from: QueryIRSource, // TODO: multiple
                       where: QueryIRNode,
                       ast: Query.Map[?, ?]) extends QueryIRSource:
  val children = Seq(project, from, where)
  val latestVar = s"subquery${QueryIRTree.idCount}"
  QueryIRTree.idCount += 1
  override def asStr = latestVar

  override def toSQLString(): String =
    s"SELECT ${project.toSQLString()} FROM ${from.toSQLString()}${where.toSQLString()}"

case class WhereClause(children: Seq[QueryIRNode], ast: DatabaseAST[?]) extends QueryIRNode:
  override def toSQLString(): String = s" WHERE ${children.map(_.toSQLString()).mkString("", " AND ", "")}"

case class PredicateExpr(child: QueryIRNode, ast: Expr.Fun[?, ?]) extends QueryIRNode:
  override val children: Seq[QueryIRNode] = Seq(child)
  override def toSQLString(): String = ???

case class BinOp(lhs: QueryIRNode, rhs: QueryIRNode, op: String, ast: Expr[?]) extends QueryIRNode:
  override val children: Seq[QueryIRNode] = Seq(lhs, rhs)
  override def toSQLString(): String = s"${lhs.toSQLString()} $op ${rhs.toSQLString()}"

case class ProjectClause(children: Seq[QueryIRNode], ast: Expr[?]) extends QueryIRNode:
  override def toSQLString(): String = children.map(_.toSQLString()).mkString("", ", ", "")

// TODO: generate anonymous names, or allow generated queries to be unnamed, or only allow named tuple results?
// ast is the parent attribute since Attr is not a node in the AST
case class AttrExpr(child: QueryIRNode, projectedName: Option[String], ast: Expr[?]) extends QueryIRNode:
  override val children: Seq[QueryIRNode] = Seq(child)
  val asStr = projectedName match
    case Some(value) => s" as $value"
    case None => ""
  override def toSQLString(): String = s"${child.toSQLString()}$asStr"

case class SelectExpr(attrName: String, from: QueryIRNode, ast: Expr[?]) extends QueryIRLeaf:
  override def toSQLString(): String = s"${from.toSQLString()}.$attrName"

case class TableLeaf(tableName: String, ast: Table[?]) extends QueryIRSource with QueryIRLeaf:
  val name = s"$tableName${QueryIRTree.idCount}"
  QueryIRTree.idCount += 1
  override def asStr = name
  override def toSQLString(): String = s"$tableName as $name"

case class QueryIRVar(toSub: QueryIRSource, name: String, ast: Expr.Ref[?]) extends QueryIRLeaf:
  override def toSQLString() = toSub.asStr

case class Literal(stringRep: String, ast: Expr[?]) extends QueryIRLeaf:
  override def toSQLString(): String = stringRep // TODO: specialize for particular platform

case class EmptyLeaf(ast: DatabaseAST[?] = null) extends QueryIRLeaf:
  override def toSQLString(): String = ""

case class PlaceHolderNode(ast: DatabaseAST[?] | Expr[?]) extends QueryIRLeaf:
  override def toSQLString(): String = s"$ast"