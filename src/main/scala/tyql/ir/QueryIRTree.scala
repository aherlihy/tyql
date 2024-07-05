package tyql

import language.experimental.namedTuples
import NamedTuple.{NamedTuple, AnyNamedTuple}
import NamedTupleDecomposition.*
/**
 * Logical query plan tree
 * Goal: move all type parameters into terms,
 */
trait QueryIRNode:
  val ast: DatabaseAST[?] // Keep AST around for debugging, TODO: probably remove
  var parent: Option[QueryIRNode] = None // TODO: might not need backwards reference
  val children: Seq[QueryIRNode]
  def toSQLString(args: String*): String

trait QueryIRSource extends QueryIRNode:
  def asStr: String

object QueryIRTree:
  var idCount = 0
  /**
   * Convert table.filter(p1).filter(p2) => table.filter(p1 && p2)
   */
  def collapseFilters(filters: Seq[QueryIRNode], comprehension: DatabaseAST[?]): (Seq[QueryIRNode], QueryIRSource) =
    comprehension match
      case table: Table[?] =>
        (filters, TableLeaf(table.$name, table))
      case filter: Query.Filter[?] =>
        collapseFilters(filters :+ PredicateExpr(filter.$pred, filter), filter.$from)
      case _ => ??? // either error or subquery, TODO

  def generateQuery(ast: DatabaseAST[?]): QueryIRNode =
    ast match
      case map: Query.Map[?,?] =>
        val (from, where) = map.$from match
          case table: Table[_] => // base case, FROM table
            (TableLeaf(table.$name, table), EmptyLeaf())
          case filter: Query.Filter[_] =>
            val (predicates, table) = collapseFilters(Seq(), filter)
            (table, WhereClause(predicates, filter))
          case _ => ??? // either error or subquery, TODO
        val attr = ProjectExpr(map.$query, map)
        SelectQuery(attr, from, where, map)
      case _ => ???

case class SelectQuery(attrs: QueryIRNode,
                       from: QueryIRSource, // TODO: multiple
                       where: QueryIRNode,
                       ast: Query.Map[?, ?]) extends QueryIRSource:
  val children = Seq(attrs, from, where)
  children.foreach(c => c.parent = Some(this))

  override def asStr = s"subquery${QueryIRTree.idCount}"
  QueryIRTree.idCount += 1

  override def toSQLString(args: String*): String =
    s"SELECT ${attrs.toSQLString(from.asStr)} FROM ${from.toSQLString()}${where.toSQLString(from.asStr)}"

case class WhereClause(children: Seq[QueryIRNode], ast: DatabaseAST[?]) extends QueryIRNode:
  children.foreach(c => c.parent = Some(this))
  override def toSQLString(args: String*): String = s" WHERE ${children.map(_.toSQLString(args.head)).mkString("", " AND ", "")}"

case class PredicateExpr(pred: Expr.Pred[?], ast: DatabaseAST[?]) extends QueryIRNode:
  val children = Seq()
  override def toSQLString(args: String*): String = s"$pred"

case class ProjectExpr(fn: Expr.Fun[?, ?], ast: DatabaseAST[?]) extends QueryIRNode:
  val children = Seq()
  val body = fn.$f

  val asAttrStr = ast.tag match
    case ResultTag.NamedTupleTag(names, types) =>
      names.map(n => s" as $n")
    case _ => ??? // for now just handle named tuples as a result

  def toStr(srcAsStr: String, expr: Expr[?]): String =
    expr match
      case s: Expr.Select[?] =>
        s"$srcAsStr.${s.$name}"
      case p: Expr.Project[?] =>
        val a = NamedTuple.toTuple(p.$a.asInstanceOf[NamedTuple[Tuple, Tuple]]) // TODO: bug?
        a.toList.zip(asAttrStr)
          .map((t, asAttr) =>
            s"${toStr(srcAsStr, t.asInstanceOf[Expr[?]])}$asAttr"
          ).mkString("", ", ", "")
      case _ => ???

  override def toSQLString(asStr: String*): String = toStr(asStr.head, body.asInstanceOf[Expr[?]])

case class TableLeaf(tableName: String, ast: Table[?]) extends QueryIRSource:
  val children = Seq()
  override def asStr = s"$tableName${QueryIRTree.idCount}"
  QueryIRTree.idCount += 1
  override def toSQLString(args: String*): String = s"$tableName as $asStr"

case class EmptyLeaf(ast: DatabaseAST[?] = null) extends QueryIRNode:
  val children = Seq()
  override def toSQLString(args: String*): String = ""
