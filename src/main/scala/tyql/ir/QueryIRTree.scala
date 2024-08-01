package tyql

import tyql.Query.Filter

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

  /**
   * Convert n subsequent calls to flatMap into an n-way join.
   * e.g. table.flatMap(t1 => table2.flatMap(t2 => table3.map(t3 => (k1 = t1, k2 = t2, k3 = t3))) =>
   *    SELECT t1 as k1, t2 as k3, t3 as k3 FROM table1, table2, table3
   */
  private def collapseFlatMap(sources: Seq[RelationOp], symbols: SymbolTable, body: DatabaseAST[?] | Expr[?]): (Seq[RelationOp], QueryIRNode) =
//    println(s"\tcollapseFlatMap: sources=${sources.map(s => s"'${s.toSQLString()}'").mkString("[", ", ", "]")}, body=$body")
    body match
      case map: Query.Map[?, ?] =>
        val srcIR = generateQuery(map.$from, symbols)
        val bodyIR = generateFun(map.$query, srcIR, symbols)
        (sources :+ srcIR, bodyIR)
      case flatMap: Query.FlatMap[?, ?] => // found a flatMap to collapse
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
      // Found an operation that cannot be collapsed
      case otherOp: DatabaseAST[?] =>
        val srcIR = generateQuery(otherOp, symbols)
        (sources :+ srcIR, ProjectClause(Seq(QueryIRVar(srcIR, srcIR.alias, null)), null))
      case _ => // Expr
        throw Exception(s"""
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

  private def unnest(tableIRs: Seq[RelationOp], projectIR: QueryIRNode, flatMap: DatabaseAST[?]): RelationOp =
    val res =
        tableIRs.reduce((q1, q2) =>
          println(s"combining '${q1.toSQLString()}' and '${q2.toSQLString()}'")
          (q1, q2) match
            case (leaf1: TableLeaf, leaf2: TableLeaf) =>
              leaf1.appendSelectAll(SelectAllQuery(Seq(leaf2), Seq(), None, flatMap), flatMap)
//              println(s"\t=> combining leafs")
//              SelectAllQuery(Seq(leaf1, leaf2), Seq(), None, flatMap)

            // table + select
            case (leaf: TableLeaf, select: SelectAllQuery) =>
              println(s"\t=> combining leaf + selectAll")
              leaf.appendSelectAll(select, flatMap)
            case (leaf: TableLeaf, select: SelectQuery) =>
              println(s"\t=> combining leaf + select")
              SelectAllQuery(Seq(q1, q2), Seq(), None, flatMap)
            case (select: SelectAllQuery, leaf: TableLeaf) =>
              println(s"\t=> combining selectAll + leaf")
              leaf.appendSelectAll(select, flatMap)
            case (select: SelectQuery, leaf: TableLeaf) =>
              println(s"\t=> combining select + leaf")
              SelectAllQuery(Seq(q1, q2), Seq(), None, flatMap)

            // two select
            case (select1: SelectAllQuery, select2: SelectAllQuery) =>
              println(s"\t=> combining selectAll + selectAll, both empty")
              select1.appendSelectAll(select2, flatMap)
            case (select1: SelectAllQuery, select2: SelectQuery) =>
              println(s"\t=> combining selectAll + select")
              SelectAllQuery(select1.from :+ select2, select1.where, None, flatMap)
            case (select1: SelectQuery, select2: SelectQuery) =>
              println(s"\t=> combining select + select, neither empty")
              SelectAllQuery(Seq(q1, q2), Seq(), None, flatMap)
            case (select1: SelectQuery, select2: SelectAllQuery) =>
              println(s"\t=> combining selectAll + select")
              SelectAllQuery(select1 +: select2.from, select2.where, None, flatMap)

            // relation op +
            case (anyOp: RelationOp, leaf: TableLeaf) =>
              println(s"\t=> combining relOp + tableLeaf")
              SelectAllQuery(Seq(q1, q2), Seq(), None, flatMap)
            case (leaf: TableLeaf, anyOp: RelationOp) =>
              println(s"\t=> combining tableLeaf + relOp")
              SelectAllQuery(Seq(q1, q2), Seq(), None, flatMap)
            case (anyOp: RelationOp, select: SelectAllQuery) =>
              println(s"\t=> combining relOp + selectAll")
              anyOp.appendSelectAll(select, flatMap)
            case (anyOp: RelationOp, select: SelectQuery) =>
              println(s"\t=> combining relOp + select")
              SelectAllQuery(Seq(q1, q2), Seq(), None, flatMap)
            case (select: SelectAllQuery, anyOp: RelationOp) =>
              println(s"\t=> combining selectAll + relOp")
              anyOp.appendSelectAll(select, flatMap)
            case (select: SelectQuery, anyOp: RelationOp) =>
              println(s"\t=> combining select + relOp")
              SelectAllQuery(Seq(q1, q2), Seq(), None, flatMap)
            case (relOp1: RelationOp, relOp2: RelationOp) =>
              println(s"\t=> combining relOp + relOp")
              SelectAllQuery(Seq(q1, q2), Seq(), None, flatMap)
            case _ =>
              println("****** EXCEPTION")
              throw new Exception(s"Cannot unnest query")
        ).appendProject(projectIR, flatMap)
    println(s"===> RETURNING '${res.toSQLString()}'")
    res


  /**
   * Generate top-level or subquery
   *
   * @param ast Query AST
   * @param symbols Symbol table, e.g. list of aliases in scope
   * @return
   */
  private def generateQuery(ast: DatabaseAST[?], symbols: SymbolTable): RelationOp =
    import TreePrettyPrinter.*
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
          generateFun(pred, tableIR, symbols) // NOTE: the parameters of all predicate functions are mapped to tableIR
        )
        val where = WhereClause(predicateExprs, filter.$pred.$body)
        tableIR match
          case s: (SelectAllQuery | TableLeaf) =>
            s.appendWhere(Seq(where), filter)
          case _ => // cannot unnest because source had project, sort, etc. TODO: some ops like limit might be unnestable
            println(s"Cannot unnest value! $tableIR")
            SelectAllQuery(Seq(tableIR), Seq(where), Some(tableIR.alias), filter)
      case flatMap: Query.FlatMap[?, ?] =>
        val sourceIR = generateQuery(flatMap.$from, symbols)
        val bodyAST = flatMap.$query
        val (tableIRs, projectIR) = collapseFlatMap(
          Seq(sourceIR),
          symbols + (bodyAST.$param.stringRef() -> sourceIR),
          bodyAST.$body
        )
        println(s"FINAL result from collapseFlatMap = ${tableIRs.map(_.toSQLString()).mkString("[\n\t", ",\n\t", "\n]")}, ${projectIR.toSQLString()}")
        import TreePrettyPrinter.*
        /** TODO: this is where could create more complex join nodes,
         * for now just r1.filter(f1).flatMap(a1 => r2.filter(f2).map(a2 => body(a1, a2))) => SELECT body FROM a1, a2 WHERE f1 AND f2
         */
        unnest(tableIRs, projectIR, flatMap)
      case aggFlatMap: Aggregation.AggFlatMap[?, ?] => // Separate bc AggFlatMap can contain Expr
        val sourceIR = generateQuery(aggFlatMap.$from, symbols)
        val outerBodyAST = aggFlatMap.$query
        val (tableIRs, projectIR) = outerBodyAST.$body match
         case recur: (Query.Map[?, ?] | Query.FlatMap[?, ?] | Aggregation.AggFlatMap[?, ?]) =>
           collapseFlatMap(
             Seq(sourceIR),
             symbols + (outerBodyAST.$param.stringRef() -> sourceIR),
             outerBodyAST.$body
           )
         case _ => // base case
            val innerBodyIR = generateFun(outerBodyAST, sourceIR, symbols)
            (Seq(sourceIR), innerBodyIR)
        unnest(tableIRs, projectIR, aggFlatMap)
      case union: Query.Union[?] =>
        val lhs = generateQuery(union.$this, symbols).appendFlag(SelectFlags.Final)
        val rhs = generateQuery(union.$other, symbols).appendFlag(SelectFlags.Final)
        val op = if union.$dedup then "UNION" else "UNION ALL"
        BinRelationOp(lhs, rhs, op, union)
      case intersect: Query.Intersect[?] =>
        val lhs = generateQuery(intersect.$this, symbols).appendFlag(SelectFlags.Final)
        val rhs = generateQuery(intersect.$other, symbols).appendFlag(SelectFlags.Final)
        BinRelationOp(lhs, rhs, "INTERSECT", intersect)
      case except: Query.Except[?] =>
        val lhs = generateQuery(except.$this, symbols).appendFlag(SelectFlags.Final)
        val rhs = generateQuery(except.$other, symbols).appendFlag(SelectFlags.Final)
        BinRelationOp(lhs, rhs, "EXCEPT", except)
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
      case l: Expr.Lower => UnaryExprOp(generateExpr(l.$x, symbols), o => s"LOWER($o)", l)
      case a: Aggregation[?] => generateAggregation(a, symbols)
      case _ => throw new Exception(s"Unimplemented Expr AST: $ast")

  private def generateAggregation(ast: Aggregation[?], symbols: SymbolTable): QueryIRNode =
    ast match
      case s: Aggregation.Sum[?] => UnaryExprOp(generateExpr(s.$a, symbols), o => s"SUM($o)", s)
      case s: Aggregation.Avg[?] => UnaryExprOp(generateExpr(s.$a, symbols), o => s"AVG($o)", s)
      case s: Aggregation.Min[?] => UnaryExprOp(generateExpr(s.$a, symbols), o => s"MIN($o)", s)
      case s: Aggregation.Max[?] => UnaryExprOp(generateExpr(s.$a, symbols), o => s"MAX($o)", s)
      case c: Aggregation.Count[?] => UnaryExprOp(generateExpr(c.$a, symbols), o => s"COUNT(1)", c)
      case p: Aggregation.AggProject[?] => generateProjection(p, symbols)
      case sub: Aggregation.AggFlatMap[?, ?] =>
        val subg = generateQuery(sub, symbols)
        subg.appendFlag(SelectFlags.ExprLevel) // special case, remove alias
      case _ => throw new Exception(s"Unimplemented aggregation op: $ast")
