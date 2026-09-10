package tyql
package dialects

/**
 * PostgreSQL's handling of a non-linear recursive term.
 *
 * PostgreSQL permits at most one reference to the recursive relation in a
 * recursive term. When TyQL generates a term that references it more than once
 * -- i.e. the query is non-linear -- the term is emitted with the working table
 * shadowed by an inner, non-recursive CTE of the same name, which is the
 * workaround the RQB PostgreSQL variants use:
 *
 * {{{
 *   WITH RECURSIVE recursive1 AS (
 *       <base>
 *     UNION
 *       (WITH recursive1 AS (SELECT * FROM recursive1) <arms>)
 *   )
 * }}}
 *
 * A linear term needs no workaround and is left alone, so the SQL for the
 * linear RQB queries is unchanged from the portable form.
 */
trait PostgresRecursion extends Dialect:
  override def recursiveTerm(cteAlias: String, term: NaryRelationOp, ast: DatabaseAST[?]): QueryIRNode =
    if QueryIRTraversal.countRefsTo(term, cteAlias) > 1 then
      InnerCTEAliasOp(cteAlias, term.appendFlag(SelectFlags.Final), ast)
    else
      term.appendFlag(SelectFlags.ExprLevel)
