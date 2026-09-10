package tyql

/**
 * IR nodes and traversals that exist to serve a particular SQL dialect. They are
 * built by [[tyql.dialects.Dialect]] hooks during IR construction, so the rest of
 * the IR stays backend-agnostic.
 */

/**
 * PostgreSQL-only: shadows the working table of a recursive CTE with an inner,
 * *non*-recursive CTE of the same name.
 *
 * PostgreSQL rejects a recursive term that names the recursive relation more
 * than once ("recursive reference to query X must not appear more than once").
 * Binding the working table once, under the same name, makes every reference in
 * the term resolve to the inner (ordinary) CTE, so the outer recursive relation
 * is referenced exactly once and a non-linear recursive query is accepted.
 */
case class InnerCTEAliasOp(cteAlias: String, child: QueryIRNode, ast: DatabaseAST[?]) extends QueryIRNode:
  override def toSQLString(): String =
    s"(WITH $cteAlias AS (SELECT * FROM $cteAlias) ${child.toSQLString()})"

  override def toString: String = s"InnerCTEAliasOp($cteAlias, $child)"

/**
 * Structural traversal of the query IR, used to decide dialect-specific
 * rendering from the shape of the query rather than from user annotations.
 */
object QueryIRTraversal:

  /**
   * The direct children of an IR node.
   *
   * Note [[QueryIRVar]] is deliberately a leaf: its `toSub` is a back-reference
   * to a relation that is already reachable from a `FROM` clause, so following
   * it would double-count and could cycle.
   */
  def children(node: QueryIRNode): Seq[QueryIRNode] = node match
    case n: WhereClause => n.children
    case n: BinExprOp => Seq(n.lhs, n.rhs)
    case n: UnaryExprOp => Seq(n.child)
    case n: ProjectClause => n.children
    case n: AttrExpr => Seq(n.child)
    case n: SelectExpr => Seq(n.from)
    case n: ListTypeExpr => n.elements
    case n: SelectAllQuery => n.from ++ n.where
    case n: SelectQuery => (n.project +: n.from) ++ n.where
    case n: OrderedQuery => n.query +: n.sortFn.map(_._1)
    case n: NaryRelationOp => n.children
    case n: GroupByQuery => (n.source +: Seq(n.groupBy)) ++ n.having
    case n: MultiRecursiveRelationOp => n.query :+ n.finalQ
    case n: InnerCTEAliasOp => Seq(n.child)
    case _ => Seq.empty // TableLeaf, RecursiveIRVar, QueryIRVar, Literal, EmptyLeaf

  /**
   * How many times the recursive relation `cteAlias` is referenced inside
   * `node`. A count above one is exactly a non-linear recursive term.
   */
  def countRefsTo(node: QueryIRNode, cteAlias: String): Int =
    val here = node match
      case v: RecursiveIRVar if v.pointsToAlias == cteAlias => 1
      case _ => 0
    here + children(node).map(countRefsTo(_, cteAlias)).sum
