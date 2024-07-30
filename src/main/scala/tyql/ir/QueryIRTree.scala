package tyql

import language.experimental.namedTuples
import NamedTuple.NamedTuple
import NamedTupleDecomposition.*

/**
 * Logical query plan tree.
 * Moves all type parameters into terms (using ResultTag).
 * Collapses nested queries where possible.
 */
object QueryIRTree:

  def generateFullQuery(ast: DatabaseAST[?], symbols: SymbolTable): RelationOp =
    generateQuery(ast, symbols).appendFlag(SelectFlags.Final) // ignore top-level parens

  var idCount = 0
  /**
   * Convert table.filter(p1).filter(p2) => table.filter(p1 && p2).
   * Example of a heuristic tree transformation/optimization
   */
  private def collapseFilters(filters: Seq[Expr.Fun[?, ?]], comprehension: DatabaseAST[?], symbols: SymbolTable): (Seq[Expr.Fun[?, ?]], RelationOp) =
    comprehension match
      case table: Table[?] =>
        (filters, TableLeaf(table.$name, table))
      case filter: Query.Filter[?] =>
        collapseFilters(filters :+ filter.$pred, filter.$from, symbols)
      case _ => (filters, generateQuery(comprehension, symbols))

  private def collapseFlatMap(sources: Seq[RelationOp], symbols: SymbolTable, body: DatabaseAST[?] | Expr[?]): (Seq[RelationOp], QueryIRNode) =
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
      case aggFlatMap: Aggregation.AggFlatMap[?, ?] => // Separate bc AggFlatMap can contain Expr
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
      case _ => throw Exception(s"""
          Unimplemented: collapsing flatMap on type $body.
          Would mean returning a row of type Row (instead of row of DB types).
          TODO: decide semantics, e.g. should it automatically flatten? Nested data types? etc.
          """)

  // TODO: probably should parametrize collapse so it works with different nodes
  private def collapseSort(sorts: Seq[(Expr.Fun[?, ?], Ord)], comprehension: DatabaseAST[?], symbols: SymbolTable): (Seq[(Expr.Fun[?, ?], Ord)], RelationOp) =
    comprehension match
      case table: Table[?] =>
        (sorts, TableLeaf(table.$name, table)) // do not reverse, since chained collection.sortBy(a).sortBy(b).sortBy(c) => ORDER BY c, b, a
      case sort: Query.Sort[?, ?] =>
        collapseSort(sorts :+ (sort.$body, sort.$ord), sort.$from, symbols)
      case _ => (sorts, generateQuery(comprehension, symbols))

  /**
   * Generate top-level or subquery
   *
   * @param ast Query AST
   * @param symbols Symbol table, e.g. list of aliases in scope
   * @return
   */
  private def generateQuery(ast: DatabaseAST[?], symbols: SymbolTable): RelationOp =
    import TreePrettyPrinter.*
//    println(s"genQuery: ast=$ast")
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
          generateFun(pred, tableIR, symbols) // NOTE: the arguments of all predicate functions are mapped to tableIR
        )
        val where = WhereClause(predicateExprs, filter.$pred.$body)
        tableIR match
          case s: SelectQuery if s.project.isEmpty =>
            tableIR.appendWhere(Seq(where), filter)
          case t: TableLeaf =>
            tableIR.appendWhere(Seq(where), filter)
          case _ => // cannot unnest because source had sort, etc. TODO: some ops like limit might be unnestable
            SelectQuery(None, Seq(tableIR), Seq(where), Some(tableIR.alias), filter)
      case flatMap: (Query.FlatMap[?, ?] | Aggregation.AggFlatMap[?, ?]) =>
        val (tableIRs, projectIR) = collapseFlatMap(Seq(), Map(), flatMap)
        import TreePrettyPrinter.*
        /** TODO: this is where could create more complex join nodes,
         * for now just r1.filter(f1).flatMap(a1 => r2.filter(f2).map(a2 => body(a1, a2))) => SELECT body FROM a1, a2 WHERE f1 AND f2
         */
        if tableIRs.length == 1 then
          tableIRs.head.appendProject(projectIR, flatMap)
        else try
          tableIRs.reduce((q1, q2) =>
//            println(s"q1=\n${q1.prettyPrintIR(0, false)}\nq2=${q2.prettyPrintIR(0,false)}")
            q2 match
              case s: SelectQuery =>
                q1.appendSubquery(s, flatMap)
              case t: TableLeaf =>
                q1.appendSubquery(
                  SelectQuery(None, Seq(t), Seq(), None, t.ast), flatMap
                )
              case _ =>
                println("Could not unnest query")
                throw new Exception(s"Cannot unnest query")
          ).appendProject(projectIR, flatMap)
        catch
          case e: Exception =>
            SelectQuery(Some(projectIR), tableIRs, Seq(), None, flatMap)

      case union: Query.Union[?] =>
        val lhs = generateQuery(union.$this, symbols).appendFlag(SelectFlags.Final)
        val rhs = generateQuery(union.$other, symbols).appendFlag(SelectFlags.Final)
        val op = if union.$dedup then "UNION" else "UNION ALL"
        BinRelationOp(lhs, rhs, op, union)
      case intersect: Query.Intersect[?] =>
        val lhs = generateQuery(intersect.$this, symbols).appendFlag(SelectFlags.Final)
        val rhs = generateQuery(intersect.$other, symbols).appendFlag(SelectFlags.Final)
        BinRelationOp(lhs, rhs, "INTERSECT", intersect)
      case sort: Query.Sort[?, ?] =>
        val (orderByASTs, tableIR) = collapseSort(Seq(), sort, symbols)
        val orderByExprs = orderByASTs.map(ord =>
          (generateFun(ord._1, tableIR, symbols), ord._2)
        )
        OrderedQuery(tableIR.appendFlag(SelectFlags.Final), orderByExprs, sort)
      case limit: Query.Limit[?] =>
        val from = generateQuery(limit.$from, symbols)
        BinRelationOp(from.appendFlag(SelectFlags.Final), Literal(limit.$limit.toString, limit.$limit), "LIMIT", limit)
      case offset: Query.Offset[?] =>
        val from = generateQuery(offset.$from, symbols)
        BinRelationOp(from.appendFlag(SelectFlags.Final), Literal(offset.$offset.toString, offset.$offset), "OFFSET", offset)
      case distinct: Query.Distinct[?] =>
        generateQuery(distinct.$from, symbols).appendFlag(SelectFlags.Distinct)
      case _ => throw new Exception(s"Unimplemented Relation-Op AST: $ast")

  private def generateFun(fun: Expr.Fun[?, ?], appliedTo: RelationOp, symbols: SymbolTable): QueryIRNode =
    fun.$body match
//      case r: Expr.Ref[?] if r.stringRef() == fun.$param.stringRef() => SelectAllExpr() // special case identity function
      case e: Expr[?] => generateExpr(e, symbols + (fun.$param.stringRef() -> appliedTo))
      case _ => ??? // TODO: find better way to differentiate

  private def generateProjection(p: Expr.Project[?] | Aggregation.AggProject[?], symbols: SymbolTable): QueryIRNode =
    val inner = p match
      case e: Expr.Project[?] => e.$a
      case a: Aggregation.AggProject[?] => a.$a
    val a = NamedTuple.toTuple(inner.asInstanceOf[NamedTuple[Tuple, Tuple]]) // TODO: bug? See https://github.com/scala/scala3/issues/21157
    val namedTupleNames = p.tag match
      case ResultTag.NamedTupleTag(names, types) => names.lift
      case _ => Seq()
    val children = a.toList.zipWithIndex
      .map((expr, idx) =>
        val e = expr.asInstanceOf[Expr[?]]
        AttrExpr(generateExpr(e, symbols), namedTupleNames(idx), e)
      )
    ProjectClause(children, p)

  private def generateExpr(ast: Expr[?], symbols: SymbolTable): QueryIRNode =
    ast match
      case ref: Expr.Ref[?] =>
        val name = ref.stringRef()
        val sub = symbols(name)
        QueryIRVar(sub, name, ref) // TODO: singleton?
      case s: Expr.Select[?] => SelectExpr(s.$name, generateExpr(s.$x, symbols), s)
      case p: Expr.Project[?] => generateProjection(p, symbols)
      case g: Expr.Gt => BinExprOp(generateExpr(g.$x, symbols), generateExpr(g.$y, symbols), ">", g)
      case g: Expr.GtDouble => BinExprOp(generateExpr(g.$x, symbols), generateExpr(g.$y, symbols), ">", g)
      case a: Expr.And => BinExprOp(generateExpr(a.$x, symbols), generateExpr(a.$y, symbols), "AND", a)
      case a: Expr.Eq => BinExprOp(generateExpr(a.$x, symbols), generateExpr(a.$y, symbols), "=", a)
      case a: Expr.Ne => BinExprOp(generateExpr(a.$x, symbols), generateExpr(a.$y, symbols), "<>", a)
      case a: Expr.Concat[?, ?] =>
        val lhsIR = generateExpr(a.$x, symbols) match
          case p: ProjectClause => p
          case v: QueryIRVar => SelectExpr("*", v, a.$x)
          case _ => throw new Exception("Unimplemented: concatting something that is not a literal nor a variable")
        val rhsIR = generateExpr(a.$y, symbols) match
          case p: ProjectClause => p
          case v: QueryIRVar => SelectExpr("*", v, a.$y)
          case _ => throw new Exception("Unimplemented: concatting something that is not a literal nor a variable")

        BinExprOp(
          lhsIR,
          rhsIR,
          ",",
          a
        )
      case l: Expr.IntLit => Literal(s"${l.$value}", l)
      case l: Expr.StringLit => Literal(s"\"${l.$value}\"", l)
      case a: Aggregation[?] => generateAggregation(a, symbols)
      case _ => throw new Exception(s"Unimplemented Expr AST: $ast")

  private def generateAggregation(ast: Aggregation[?], symbols: SymbolTable): QueryIRNode =
    ast match
      case s: Aggregation.Sum[?] => UnaryExprOp(generateExpr(s.$a, symbols), o => s"SUM($o)", s)
      case s: Aggregation.Avg[?] => UnaryExprOp(generateExpr(s.$a, symbols), o => s"AVG($o)", s)
      case s: Aggregation.Min[?] => UnaryExprOp(generateExpr(s.$a, symbols), o => s"MIN($o)", s)
      case s: Aggregation.Max[?] => UnaryExprOp(generateExpr(s.$a, symbols), o => s"MAX($o)", s)
      case p: Aggregation.AggProject[?] => generateProjection(p, symbols)
      case sub: Aggregation.AggFlatMap[?, ?] =>
        val subg = generateQuery(sub, symbols)
        subg.appendFlag(SelectFlags.ExprLevel) // special case, remove alias
      case _ => throw new Exception(s"Unimplemented aggregation op: $ast")
