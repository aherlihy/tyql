package tyql

/** Nodes in the query IR tree, representing expressions / subclauses
  */
trait QueryIRNode:
  val ast
    : DatabaseAST[?] | Expr[?, ?] | Expr.Fun[
      ?,
      ?,
      ?
    ] // Best-effort, keep AST around for debugging, TODO: probably remove, or replace only with ResultTag

  def toSQLString(): String

trait QueryIRLeaf extends QueryIRNode

/** Single WHERE clause containing 1+ predicates
  */
case class WhereClause(children: Seq[QueryIRNode], ast: Expr[?, ?]) extends QueryIRNode:
  override def toSQLString(): String = if children.size == 1 then children.head.toSQLString()
  else s"${children.map(_.toSQLString()).mkString("", " AND ", "")}"

/** Binary expression-level operation. TODO: cannot assume the operation is universal, need to specialize for DB backend
  */
case class BinExprOp(lhs: QueryIRNode, rhs: QueryIRNode, op: (String, String) => String, ast: Expr[?, ?])
    extends QueryIRNode:
  override def toSQLString(): String = op(lhs.toSQLString(), rhs.toSQLString())

/** Unary expression-level operation. TODO: cannot assume the operation is universal, need to specialize for DB backend
  */
case class UnaryExprOp(child: QueryIRNode, op: String => String, ast: Expr[?, ?]) extends QueryIRNode:
  override def toSQLString(): String = op(s"${child.toSQLString()}")

/** Project clause, e.g. SELECT <...> FROM
  * @param children
  * @param ast
  */
case class ProjectClause(children: Seq[QueryIRNode], ast: Expr[?, ?]) extends QueryIRNode:
  override def toSQLString(): String = children.map(_.toSQLString()).mkString("", ", ", "")

/** Named or unnamed attribute select expression, e.g. `table.rowName as customName` TODO: generate anonymous names, or
  * allow generated queries to be unnamed, or only allow named tuple results? Note projected attributes with names is
  * not the same as aliasing, and just exists for readability
  */
case class AttrExpr(child: QueryIRNode, projectedName: Option[String], ast: Expr[?, ?]) extends QueryIRNode:
  val asStr = projectedName match
    case Some(value) => s" as $value"
    case None        => ""
  override def toSQLString(): String = s"${child.toSQLString()}$asStr"

/** Attribute access expression, e.g. `table.rowName`.
  */
case class SelectExpr(attrName: String, from: QueryIRNode, ast: Expr[?, ?]) extends QueryIRLeaf:
  override def toSQLString(): String = s"${from.toSQLString()}.$attrName"

/** A variable that points to a table or subquery.
  */
case class QueryIRVar(toSub: RelationOp, name: String, ast: Expr.Ref[?, ?]) extends QueryIRLeaf:
  override def toSQLString() = toSub.alias

  override def toString: String = s"VAR(${toSub.alias}.$name)"

/** Literals. TODO: can't assume stringRep is universal, need to specialize for DB backend.
  */
case class Literal(stringRep: String, ast: Expr[?, ?]) extends QueryIRLeaf:
  override def toSQLString(): String = stringRep

case class ListTypeExpr(elements: List[QueryIRNode], ast: Expr[?, ?]) extends QueryIRNode:
  override def toSQLString(): String = elements.map(_.toSQLString()).mkString("[", ", ", "]")

/** Empty leaf node, to avoid Options everywhere.
  */
case class EmptyLeaf(ast: DatabaseAST[?] = null) extends QueryIRLeaf:
  override def toSQLString(): String = ""
