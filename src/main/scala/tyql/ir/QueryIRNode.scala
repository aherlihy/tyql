package tyql

/**
 * Nodes in the query IR tree, representing expressions / subclauses
 */
trait QueryIRNode:
  val ast: DatabaseAST[?] | Expr[?, ?] | Expr.Fun[?, ?, ?] // Best-effort, keep AST around for debugging, TODO: probably remove, or replace only with ResultTag

  val precedence: Int = Precedence.Default
  private var cached: java.util.concurrent.ConcurrentHashMap[(Dialect, Config), String] = null // do not allocate memory if unused

  final def toSQLString(using d: Dialect)(using cnf: Config)(): String =
    if cached == null then
      this.synchronized {
        if cached == null then
          cached = new java.util.concurrent.ConcurrentHashMap[(Dialect, Config), String]()
      }
    cached.computeIfAbsent((d, cnf), _ => computeSQLString(using d)(using cnf)())

  protected def computeSQLString(using d: Dialect)(using cnf: Config)(): String

trait QueryIRLeaf extends QueryIRNode

// TODO can we source it from somewhere and not guess about this?
// Current values were proposed on 2024-11-19 by Claude Sonnet 3.5 v20241022, and somewhat modifier later
object Precedence {
  val Literal        = 100  // literals, identifiers
  val ListOps        = 95   // list_append, list_prepend, list_contains
  val Unary          = 90   // NOT, EXIST, etc
  val Multiplicative = 80   // *, /
  val Additive       = 70   // +, -
  val Comparison     = 60   // =, <>, <, >, <=, >=
  val And            = 50   // AND
  val Or             = 40   // OR
  val Concat         = 10   // , in select clause
  val Default        = 0
}

/**
 * Single WHERE clause containing 1+ predicates
 */
case class WhereClause(children: Seq[QueryIRNode], ast: Expr[?, ?]) extends QueryIRNode:
  override def computeSQLString(using d: Dialect)(using cnf: Config)(): String = if children.size == 1 then children.head.toSQLString() else  s"${children.map(_.toSQLString()).mkString("", " AND ", "")}"

/**
 * Binary expression-level operation.
 * TODO: cannot assume the operation is universal, need to specialize for DB backend
 */
case class BinExprOp(lhs: QueryIRNode, rhs: QueryIRNode, op: (String, String) => String, override val precedence: Int, ast: Expr[?, ?]) extends QueryIRNode:
  override def computeSQLString(using d: Dialect)(using cnf: Config)(): String =
    val leftStr = if needsParens(lhs) then s"(${lhs.toSQLString()})" else lhs.toSQLString()
    val rightStr = if needsParens(rhs) then s"(${rhs.toSQLString()})" else rhs.toSQLString()
    op(leftStr, rightStr)

  private def needsParens(node: QueryIRNode): Boolean =
    // TODO should this be specialized into needsLeftParen and needsRightParen?
    //      Unclear if it would be used since we generate from Scala expressions...
    node.precedence != 0 && node.precedence < this.precedence

/**
 * Unary expression-level operation.
 * TODO: cannot assume the operation is universal, need to specialize for DB backend
 */
case class UnaryExprOp(child: QueryIRNode, op: String => String, ast: Expr[?, ?]) extends QueryIRNode:
  override val precedence: Int = Precedence.Unary
  override def computeSQLString(using d: Dialect)(using cnf: Config)(): String = op(s"${child.toSQLString()}")

case class FunctionCallOp(name: String, children: Seq[QueryIRNode], ast: Expr[?, ?]) extends QueryIRNode:
  override val precedence = Precedence.Literal
  override def computeSQLString(using d: Dialect)(using cnf: Config)(): String = s"$name(" + children.map(_.toSQLString()).mkString(", ") + ")"
  // TODO does this need ()s sometimes?

case class SearchedCaseOp(whenClauses: Seq[(QueryIRNode, QueryIRNode)], elseClause: Option[QueryIRNode], ast: Expr[?, ?]) extends QueryIRNode:
  override val precedence = Precedence.Literal
  override def computeSQLString(using d: Dialect)(using cnf: Config)(): String =
    val whenStr = whenClauses.map { case (cond, res) => s"WHEN ${cond.toSQLString()} THEN ${res.toSQLString()}" }.mkString(" ")
    val elseStr = elseClause.map(e => s" ELSE ${e.toSQLString()}").getOrElse("")
    s"CASE $whenStr$elseStr END"

case class SimpleCaseOp(expr: QueryIRNode, whenClauses: Seq[(QueryIRNode, QueryIRNode)], elseClause: Option[QueryIRNode], ast: Expr[?, ?]) extends QueryIRNode:
  override val precedence = Precedence.Literal
  override def computeSQLString(using d: Dialect)(using cnf: Config)(): String =
    val exprStr = expr.toSQLString()
    val whenStr = whenClauses.map { case (cond, res) => s"WHEN ${cond.toSQLString()} THEN ${res.toSQLString()}" }.mkString(" ")
    val elseStr = elseClause.map(e => s" ELSE ${e.toSQLString()}").getOrElse("")
    s"CASE $exprStr $whenStr$elseStr END"

case class RawSQLInsertOp(sql: String, replacements: Map[String, QueryIRNode], override val precedence: Int, ast: Expr[?, ?]) extends QueryIRNode:
  override def computeSQLString(using d: Dialect)(using cnf: Config)(): String =
    replacements.foldLeft(sql) { case (acc, (k, v)) => acc.replace(k, v.toSQLString()) }

/**
 * Project clause, e.g. SELECT <...> FROM
 * @param children
 * @param ast
 */
case class ProjectClause(children: Seq[QueryIRNode], ast: Expr[?, ?]) extends QueryIRNode:
  override def computeSQLString(using d: Dialect)(using cnf: Config)(): String = children.map(_.toSQLString()).mkString("", ", ", "")


/**
 * Named or unnamed attribute select expression, e.g. `table.rowName as customName`
 * TODO: generate anonymous names, or allow generated queries to be unnamed, or only allow named tuple results?
 * Note projected attributes with names is not the same as aliasing, and just exists for readability
 */
case class AttrExpr(child: QueryIRNode, projectedName: Option[String], ast: Expr[?, ?])(using d: Dialect) extends QueryIRNode:
  val asStr = projectedName match
    case Some(value) => s" as ${d.quoteIdentifier(value)}"
    case None => ""
  override def computeSQLString(using d: Dialect)(using cnf: Config)(): String = s"${child.toSQLString()}$asStr"

/**
 * Attribute access expression, e.g. `table.rowName`.
 */
case class SelectExpr(attrName: String, from: QueryIRNode, ast: Expr[?, ?]) extends QueryIRLeaf:
  override def computeSQLString(using d: Dialect)(using cnf: Config)(): String =
    s"${from.toSQLString()}.${d.quoteIdentifier(cnf.caseConvention.convert(attrName))}"

/**
 * A variable that points to a table or subquery.
 */
case class QueryIRVar(toSub: RelationOp, name: String, ast: Expr.Ref[?, ?]) extends QueryIRLeaf:
  override def computeSQLString(using d: Dialect)(using cnf: Config)() =
    d.quoteIdentifier(cnf.caseConvention.convert(toSub.alias))

  override def toString: String = s"VAR(${toSub.alias}.${name})" // TODO what about this?

/**
 * Literals.
 * TODO: can't assume stringRep is universal, need to specialize for DB backend.
 */
case class Literal(stringRep: String, ast: Expr[?, ?]) extends QueryIRLeaf:
  override val precedence: Int = Precedence.Literal
  override def computeSQLString(using d: Dialect)(using cnf: Config)(): String = stringRep

case class ListTypeExpr(elements: List[QueryIRNode], ast: Expr[?, ?]) extends QueryIRNode:
  override val precedence: Int = Precedence.Literal
  override def computeSQLString(using d: Dialect)(using cnf: Config)(): String = elements.map(_.toSQLString()).mkString("[", ", ", "]")

/**
 * Empty leaf node, to avoid Options everywhere.
 */
case class EmptyLeaf(ast: DatabaseAST[?] = null) extends QueryIRLeaf:
  override def computeSQLString(using d: Dialect)(using cnf: Config)(): String = ""
