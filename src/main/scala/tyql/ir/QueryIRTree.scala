package tyql

import language.experimental.namedTuples
import NamedTuple.{NamedTuple, AnyNamedTuple}
/**
 * Logical query plan tree
 * Goal: move all type parameters into terms,
 */
trait QueryIRNode:
  val ast: DatabaseAST[?] // Keep AST around for debugging, TODO: probably remove
  var parent: Option[QueryIRNode] = None // TODO: might not need backwards reference
  val children: Seq[QueryIRNode]
  def toSQLString: String

object QueryIRTree:
  var idCount = 0
  /**
   * Convert table.filter(p1).filter(p2) => table.filter(p1 && p2)
   */
  def collapseFilters(filters: Seq[QueryIRNode], comprehension: DatabaseAST[?]): (Seq[QueryIRNode], QueryIRNode) =
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
        val attr = SelectExpr(map.$query, map)
        SelectQuery(attr, from, where, map)
      case _ => ???

case class SelectQuery(attrs: QueryIRNode,
                       from: QueryIRNode, // TODO: multiple
                       where: QueryIRNode,
                       ast: Query.Map[?, ?]) extends QueryIRNode:
  val children = Seq(attrs, from, where)
  children.foreach(c => c.parent = Some(this))

  val as = s"subquery${QueryIRTree.idCount}"
  QueryIRTree.idCount += 1
  override def toSQLString: String =
    s"tag=${ast.tag}: SELECT ${attrs.toSQLString} FROM ${from.toSQLString}${where.toSQLString}"

case class WhereClause(children: Seq[QueryIRNode], ast: DatabaseAST[?]) extends QueryIRNode:
  children.foreach(c => c.parent = Some(this))
  override def toSQLString: String = s" WHERE ${children.map(_.toSQLString).mkString("", " AND ", "")}"

case class PredicateExpr(pred: Expr.Pred[?], ast: DatabaseAST[?]) extends QueryIRNode:
  val children = Seq()
  override def toSQLString: String = s"$pred"

case class SelectExpr(fn: Expr.Fun[?, ?], ast: DatabaseAST[?]) extends QueryIRNode:
  val children = Seq()
//  val names = taggedName.names.map(n => s"as $n")
  def selectStr(expr: Expr[?]): String =
    expr match
      case s: Expr.Select[?] =>
        s"${parent.get.asInstanceOf[SelectQuery].from.asInstanceOf[TableLeaf].as}.${s.$name}"
      case p: Expr.Project[?] =>
        val a = p.$a
        s"A=${a}"
      case _ => ???

  override def toSQLString: String = s"${selectStr(fn.$f.asInstanceOf[Expr[?]])}"

case class TableLeaf(tableName: String, ast: Table[?]) extends QueryIRNode:
  val children = Seq()
  val as = s"$tableName${QueryIRTree.idCount}"
  QueryIRTree.idCount += 1
  override def toSQLString: String = s"$tableName as $as"

case class EmptyLeaf(ast: DatabaseAST[?] = null) extends QueryIRNode:
  val children = Seq()
  override def toSQLString: String = ""
