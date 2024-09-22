package tyql

import tyql.Query.{Intersect, QueryRef}
import tyql.ResultTag.NamedTupleTag

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
  private def collapseFilters(filters: Seq[Expr.Fun[?, ?, ?]], comprehension: DatabaseAST[?], symbols: SymbolTable): (Seq[Expr.Fun[?, ?, ?]], DatabaseAST[?]) =
    comprehension match
      case filter: Query.Filter[?, ?] =>
        collapseFilters(filters :+ filter.$pred, filter.$from, symbols)
      case aggFilter: tyql.Aggregation.AggFilter[?] =>
        collapseFilters(filters :+ aggFilter.$pred, aggFilter.$from, symbols)
      case _ => (filters, comprehension)

  private def lookupRecursiveRef(actualParam: RelationOp, newRef: String): RelationOp =
    actualParam match
      case RecursiveIRVar(pointsToAlias, alias, ast) =>
        RecursiveIRVar(pointsToAlias, newRef, ast)
      case p => p

  /**
   * Convert n subsequent calls to flatMap into an n-way join.
   * e.g. table.flatMap(t1 => table2.flatMap(t2 => table3.map(t3 => (k1 = t1, k2 = t2, k3 = t3))) =>
   *    SELECT t1 as k1, t2 as k3, t3 as k3 FROM table1, table2, table3
   */
  private def collapseFlatMap(sources: Seq[RelationOp], symbols: SymbolTable, body: Any): (Seq[RelationOp], QueryIRNode) =
    body match
      case map: Query.Map[?, ?] =>
        val actualParam = generateActualParam(map.$from, map.$query.$param, symbols)
        val bodyIR = generateFun(map.$query, actualParam, symbols)
        (sources :+ actualParam, bodyIR)
      case flatMap: Query.FlatMap[?, ?] => // found a flatMap to collapse
        val actualParam = generateActualParam(flatMap.$from, flatMap.$query.$param, symbols)
        val (unevaluated, boundST) = partiallyGenerateFun(flatMap.$query, actualParam, symbols)
        collapseFlatMap(
          sources :+ actualParam,
          boundST,
          unevaluated
        )
      case aggFlatMap: Aggregation.AggFlatMap[?, ?] => // Separate bc AggFlatMap can contain Expr
        val actualParam = generateActualParam(aggFlatMap.$from, aggFlatMap.$query.$param, symbols)
        val (unevaluated, boundST) = partiallyGenerateFun(aggFlatMap.$query, actualParam, symbols)
        unevaluated match
          case recur: (Query.Map[?, ?] | Query.FlatMap[?, ?] | Aggregation.AggFlatMap[?, ?]) =>
            collapseFlatMap(
              sources :+ actualParam,
              boundST,
              recur
            )
          case _ => // base case
            val innerBodyIR = finishGeneratingFun(unevaluated, boundST)
            (sources :+ actualParam, innerBodyIR)
      // Found an operation that cannot be collapsed
      case otherOp: DatabaseAST[?] =>
        val fromNode = generateQuery(otherOp, symbols)
        (sources :+ fromNode, ProjectClause(Seq(QueryIRVar(fromNode, fromNode.alias, null)), null))
      case _ => // Expr
        throw Exception(s"""
            Unimplemented: collapsing flatMap on type $body.
            Would mean returning a row of type Row (instead of row of DB types).
            TODO: decide semantics, e.g. should it automatically flatten? Nested data types? etc.
            """)


  private def collapseSort(sorts: Seq[(Expr.Fun[?, ?, ?], Ord)], comprehension: DatabaseAST[?], symbols: SymbolTable): (Seq[(Expr.Fun[?, ?, ?], Ord)], DatabaseAST[?]) =
    comprehension match
      case sort: Query.Sort[?, ?, ?] =>
        collapseSort(sorts :+ (sort.$body, sort.$ord), sort.$from, symbols)
      case _ => (sorts, comprehension)

  // TODO: verify set vs. bag operator precendence is preserved in the generated queries
  private def collapseNaryOp(lhs: QueryIRNode, rhs: QueryIRNode, op: String, ast: DatabaseAST[?]): NaryRelationOp =
    val flattened = (
      lhs match
        case NaryRelationOp(lhsChildren, lhsOp, lhsAst) if op == lhsOp =>
          lhsChildren
        case _ => Seq(lhs)
      ) ++ (
      rhs match
        case NaryRelationOp(rhsChildren, rhsOp, rhsAST) if op == rhsOp =>
          rhsChildren
        case _ => Seq(rhs)
      )

    NaryRelationOp(flattened, op, ast)

  private def collapseMap(lhs: QueryIRNode, rhs: QueryIRNode): RelationOp =
    ???

  private def unnest(fromNodes: Seq[RelationOp], projectIR: QueryIRNode, flatMap: DatabaseAST[?]): RelationOp =
    fromNodes.reduce((q1, q2) =>
      val res = q1.mergeWith(q2, flatMap)
      res
    ).appendProject(projectIR, flatMap)

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
        val actualParam = generateActualParam(map.$from, map.$query.$param, symbols)
        val attrNode = generateFun(map.$query, actualParam, symbols)
        actualParam.appendProject(attrNode, map)
      case filter: Query.Filter[?, ?] =>
        val (predicateASTs, fromNodeAST) = collapseFilters(Seq(), filter, symbols)
        val actualParam = generateActualParam(fromNodeAST, filter.$pred.$param, symbols)
        val predicateExprs = predicateASTs.map(pred =>
          generateFun(pred, actualParam, symbols)
        )
        val where = WhereClause(predicateExprs, filter.$pred.$body)
        actualParam.appendWhere(where, filter)
      case filter: Aggregation.AggFilter[?] =>
        val (predicateASTs, fromNodeAST) = collapseFilters(Seq(), filter, symbols)
        val actualParam = generateActualParam(fromNodeAST, filter.$pred.$param, symbols)
        val predicateExprs = predicateASTs.map(pred =>
          generateFun(pred, actualParam, symbols)
        )
        val where = WhereClause(predicateExprs, filter.$pred.$body)
        actualParam.appendWhere(where, filter)  
      case sort: Query.Sort[?, ?, ?] =>
        val (orderByASTs, fromNodeAST) = collapseSort(Seq(), sort, symbols)
        val actualParam = generateActualParam(fromNodeAST, sort.$body.$param, symbols)
        val orderByExprs = orderByASTs.map(ord =>
          val res = generateFun(ord._1, actualParam, symbols)
          (res, ord._2)
        )
        OrderedQuery(actualParam.appendFlag(SelectFlags.Final), orderByExprs, sort)
      case flatMap: Query.FlatMap[?, ?] =>
        val actualParam = generateActualParam(flatMap.$from, flatMap.$query.$param, symbols)
        val (unevaluated, boundST) = partiallyGenerateFun(flatMap.$query, actualParam, symbols)
        val (fromNodes, projectIR) = collapseFlatMap(
          Seq(actualParam),
          boundST,
          unevaluated
        )
        import TreePrettyPrinter.*
        /** TODO: this is where could create more complex join nodes,
         * for now just r1.filter(f1).flatMap(a1 => r2.filter(f2).map(a2 => body(a1, a2))) => SELECT body FROM a1, a2 WHERE f1 AND f2
         */
        unnest(fromNodes, projectIR, flatMap)
      case aggFlatMap: Aggregation.AggFlatMap[?, ?] => // Separate bc AggFlatMap can contain Expr
        val actualParam = generateActualParam(aggFlatMap.$from, aggFlatMap.$query.$param, symbols)
        val (unevaluated, boundST) = partiallyGenerateFun(aggFlatMap.$query, actualParam, symbols)
        val (fromNodes, projectIR) = unevaluated match
         case recur: (Query.Map[?, ?] | Query.FlatMap[?, ?] | Aggregation.AggFlatMap[?, ?]) =>
           collapseFlatMap(
             Seq(actualParam),
             boundST,
             recur
           )
         case _ => // base case
            val innerBodyIR = finishGeneratingFun(unevaluated, boundST)
            (Seq(actualParam), innerBodyIR)
        unnest(fromNodes, projectIR, aggFlatMap)
      case relOp: (Query.Union[?] | Query.UnionAll[?] | Query.Intersect[?] | Query.IntersectAll[?] | Query.Except[?] | Query.ExceptAll[?]) =>
        val (thisN, thatN, category, op) = relOp match
          case set: Query.Union[?] => (set.$this, set.$other, "", "UNION")
          case bag: Query.UnionAll[?] => (bag.$this, bag.$other, " ALL", "UNION")
          case set: Query.Intersect[?] => (set.$this, set.$other, "", "INTERSECT")
          case bag: Query.IntersectAll[?] => (bag.$this, bag.$other, " ALL", "INTERSECT")
          case set: Query.Except[?] => (set.$this, set.$other, "", "EXCEPT")
          case bag: Query.ExceptAll[?] => (bag.$this, bag.$other, " ALL", "EXCEPT")
        val lhs = generateQuery(thisN, symbols).appendFlag(SelectFlags.ExprLevel)
        val rhs = generateQuery(thatN, symbols).appendFlag(SelectFlags.ExprLevel)
        collapseNaryOp(lhs, rhs, s"$op$category", relOp)
      case limit: Query.Limit[?, ?] =>
        val from = generateQuery(limit.$from, symbols)
        collapseNaryOp(from.appendFlag(SelectFlags.Final), Literal(limit.$limit.toString, Expr.IntLit(limit.$limit)), "LIMIT", limit)
      case offset: Query.Offset[?, ?] =>
        val from = generateQuery(offset.$from, symbols)
        collapseNaryOp(from.appendFlag(SelectFlags.Final), Literal(offset.$offset.toString, Expr.IntLit(offset.$offset)), "OFFSET", offset)
      case distinct: Query.Distinct[?] =>
        generateQuery(distinct.$from, symbols).appendFlag(SelectFlags.Distinct)
      case queryRef: Query.QueryRef[?, ?] =>
        symbols(queryRef.stringRef())
      case multiRecursive: Query.MultiRecursive[?] =>
        val vars = multiRecursive.$param.map(rP =>
          val p = rP.toQueryRef
          QueryIRTree.idCount += 1
          val newAlias = s"recursive${QueryIRTree.idCount}"

          // assign variable ID to alias in symbol table
          val varId = p.stringRef()
          val variable = RecursiveIRVar(newAlias, varId, p)
          (varId, variable)
        )
        val allSymbols = symbols.bind(vars)
        val aliases = vars.map(v => v._2.pointsToAlias)
        val subqueriesIR = multiRecursive.$subquery.map(q => generateQuery(q, allSymbols).appendFlag(SelectFlags.Final))
        // Special case to enforce parens around recursive definitions
        val separatedSQ = subqueriesIR.map {
          case union: NaryRelationOp =>
            val base = union.children.head
            val recur = union.children.tail
            val recurIR = NaryRelationOp(recur, union.op, union.ast).appendFlag(SelectFlags.ExprLevel)
            NaryRelationOp(Seq(base, recurIR), union.op, union.ast).appendFlags(union.flags)
          case _ => throw new Exception("Unexpected non-union subtree of recursive query")
        }

        val finalQ = multiRecursive.$resultQuery match
          case ref: QueryRef[?, ?] =>
            val v = vars.find((id, _) => id == ref.stringRef()).get._2
            SelectAllQuery(Seq(v), Seq(), Some(v.alias), multiRecursive.$resultQuery)
          case q => ??? //generateQuery(q, allSymbols, multiRecursive.$resultQuery)

        MultiRecursiveRelationOp(aliases, separatedSQ, finalQ.appendFlag(SelectFlags.Final), multiRecursive)

      case groupBy: Query.GroupBy[?, ?, ?, ?] =>
        val fromIR = generateQuery(groupBy.$source, symbols)

        def getSource(f: RelationOp): RelationOp = f match
          case SelectQuery(project, from, where, overrideAlias, ast) =>
            val select = generateFun(groupBy.$selectFn, fromIR, symbols)
            SelectQuery(collapseMap(select, project), from, where, None, groupBy)
          case SelectAllQuery(from, where, overrideAlias, ast) =>
            val select = generateFun(groupBy.$selectFn, fromIR, symbols)
            SelectQuery(select, from, where, None, groupBy)
          case MultiRecursiveRelationOp(aliases, query, finalQ, ast) =>
            val newSource = getSource(finalQ).appendFlags(finalQ.flags)
            MultiRecursiveRelationOp(aliases, query, newSource, ast)
          case _ =>
            val select = generateFun(groupBy.$selectFn, fromIR, symbols)
            SelectQuery(select, Seq(fromIR), Seq(), None, groupBy) // force subquery

        val source = getSource(fromIR)

        // Turn off the attribute names for the grouping clause since no aliases needed
        groupBy.$groupingFn.$body.tag match
          case t: NamedTupleTag[?, ?] => t.names = List()
          case _ =>
        val groupByIR = generateFun(groupBy.$groupingFn, fromIR, symbols)
        val havingIR = groupBy.$havingFn.map(f => generateFun(f, fromIR, symbols))
        GroupByQuery(source.appendFlag(SelectFlags.Final), groupByIR, havingIR, overrideAlias = None, ast = groupBy)

      case _ => throw new Exception(s"Unimplemented Relation-Op AST: $ast")


  private def generateActualParam(from: DatabaseAST[?], formalParam: Expr.Ref[?, ?], symbols: SymbolTable): RelationOp =
    lookupRecursiveRef(generateQuery(from, symbols), formalParam.stringRef())
  /**
   * Generate the actual parameter expression and bind it to the formal parameter in the symbol table, but
   * leave the function body uncompiled.
   */
  private def partiallyGenerateFun(fun: Expr.Fun[?, ?, ?], actualParam: RelationOp, symbols: SymbolTable): (Any, SymbolTable) =
    val boundST = symbols.bind(fun.$param.stringRef(), actualParam)
    (fun.$body, boundST)

  /**
   * Compile the function body.
   */
  private def finishGeneratingFun(funBody: Any, boundST: SymbolTable): QueryIRNode =
    funBody match
      //      case r: Expr.Ref[?] if r.stringRef() == fun.$param.stringRef() => SelectAllExpr() // special case identity function
      case e: Expr[?, ?] => generateExpr(e, boundST)
      case d: DatabaseAST[?] => generateQuery(d, boundST)
      case _ => ??? // TODO: find better way to differentiate

  /**
   * Generate a function, return the actual parameter and the function body.
   * Sometimes, want to split this function into separate steps, for the cases where you want to collate multiple
   * function bodies within a single expression.
   */
  private def generateFun(fun: Expr.Fun[?, ?, ?], appliedTo: RelationOp, symbols: SymbolTable): QueryIRNode =
    val (body, boundSymbols) = partiallyGenerateFun(fun, appliedTo, symbols)
    finishGeneratingFun(body, boundSymbols)


  private def generateProjection(p: Expr.Project[?] | AggregationExpr.AggProject[?], symbols: SymbolTable): QueryIRNode =
    val projectAST = p match
      case e: Expr.Project[?] => e.$a
      case a: AggregationExpr.AggProject[?] => a.$a
    val tupleOfProject = NamedTuple.toTuple(projectAST.asInstanceOf[NamedTuple[Tuple, Tuple]]) // TODO: bug? See https://github.com/scala/scala3/issues/21157
    val namedTupleNames = p.tag match
      case ResultTag.NamedTupleTag(names, types) => names.lift
      case _ => Seq()
    val children = tupleOfProject.toList.zipWithIndex
      .map((expr, idx) =>
        val e = expr.asInstanceOf[Expr[?, ?]]
        AttrExpr(generateExpr(e, symbols), namedTupleNames(idx), e)
      )
    ProjectClause(children, p)

  private def generateExpr(ast: Expr[?, ?], symbols: SymbolTable): QueryIRNode =
    ast match
      case ref: Expr.Ref[?, ?] =>
        val name = ref.stringRef()
        val sub = symbols(name)
        QueryIRVar(sub, name, ref) // TODO: singleton?
      case s: Expr.Select[?] => SelectExpr(s.$name, generateExpr(s.$x, symbols), s)
      case p: Expr.Project[?] => generateProjection(p, symbols)
      case g: Expr.Gt[?, ?] => BinExprOp(generateExpr(g.$x, symbols), generateExpr(g.$y, symbols), (l, r) => s"$l > $r", g)
      case g: Expr.Lt[?, ?] => BinExprOp(generateExpr(g.$x, symbols), generateExpr(g.$y, symbols), (l, r) => s"$l < $r", g)
      case g: Expr.GtDouble[?, ?] => BinExprOp(generateExpr(g.$x, symbols), generateExpr(g.$y, symbols), (l, r) => s"$l > $r", g)
      case a: Expr.And[?, ?] => BinExprOp(generateExpr(a.$x, symbols), generateExpr(a.$y, symbols), (l, r) => s"$l AND $r", a)
      case n: Expr.Not[?] => UnaryExprOp(generateExpr(n.$x, symbols), o => s"NOT $o", n)
      case a: Expr.Plus[?, ?, ?] => BinExprOp(generateExpr(a.$x, symbols), generateExpr(a.$y, symbols), (l, r) => s"$l + $r", a)
      case a: Expr.Eq[?, ?] => BinExprOp(generateExpr(a.$x, symbols), generateExpr(a.$y, symbols), (l, r) => s"$l = $r", a)
      case a: Expr.Ne[?, ?] => BinExprOp(generateExpr(a.$x, symbols), generateExpr(a.$y, symbols), (l, r) => s"$l <> $r", a)
      case a: Expr.Concat[?, ?, ?, ?] =>
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
          (l, r) => s"$l, $r",
          a
        )
      case l: Expr.IntLit => Literal(s"${l.$value}", l)
      case l: Expr.StringLit => Literal(s"\"${l.$value}\"", l)
      case l: Expr.BooleanLit => Literal(s"\"${l.$value}\"", l)
      case l: Expr.Lower[?] => UnaryExprOp(generateExpr(l.$x, symbols), o => s"LOWER($o)", l)
      case a: AggregationExpr[?] => generateAggregation(a, symbols)
      case a: Aggregation[?] => generateQuery(a, symbols).appendFlag(SelectFlags.ExprLevel)
      case list: Expr.ListExpr[?] => ListTypeExpr(list.$elements.map(generateExpr(_, symbols)), list)
      case p: Expr.ListPrepend[?] => BinExprOp(generateExpr(p.$x, symbols), generateExpr(p.$list, symbols), (l, r) => s"list_prepend($l, $r)", p)
      case p: Expr.ListAppend[?] => BinExprOp(generateExpr(p.$list, symbols), generateExpr(p.$x, symbols),(l, r) => s"list_append($l, $r)", p)
      case p: Expr.ListContains[?] => BinExprOp(generateExpr(p.$list, symbols), generateExpr(p.$x, symbols),(l, r) => s"list_contains($l, $r)", p)
      case p: Expr.ListLength[?] => UnaryExprOp(generateExpr(p.$list, symbols), s => s"length($s)", p)
      case p: Expr.NonEmpty[?] => UnaryExprOp(generateQuery(p.$this, symbols).appendFlag(SelectFlags.Final), s => s"EXISTS ($s)", p)
      case p: Expr.IsEmpty[?] => UnaryExprOp(generateQuery(p.$this, symbols).appendFlag(SelectFlags.Final), s => s"NOT EXISTS ($s)", p)
      case _ => throw new Exception(s"Unimplemented Expr AST: $ast")

  private def generateAggregation(ast: AggregationExpr[?], symbols: SymbolTable): QueryIRNode =
    ast match
      case s: AggregationExpr.Sum[?] => UnaryExprOp(generateExpr(s.$a, symbols), o => s"SUM($o)", s)
      case s: AggregationExpr.Avg[?] => UnaryExprOp(generateExpr(s.$a, symbols), o => s"AVG($o)", s)
      case s: AggregationExpr.Min[?] => UnaryExprOp(generateExpr(s.$a, symbols), o => s"MIN($o)", s)
      case s: AggregationExpr.Max[?] => UnaryExprOp(generateExpr(s.$a, symbols), o => s"MAX($o)", s)
      case c: AggregationExpr.Count[?] => UnaryExprOp(generateExpr(c.$a, symbols), o => s"COUNT(1)", c)
      case p: AggregationExpr.AggProject[?] => generateProjection(p, symbols)
      case _ => throw new Exception(s"Unimplemented aggregation op: $ast")
