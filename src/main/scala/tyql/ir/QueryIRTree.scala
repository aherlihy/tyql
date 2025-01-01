package tyql

import tyql.Query.{Intersect, QueryRef}
import tyql.ResultTag.NamedTupleTag

import language.experimental.namedTuples
import NamedTuple.NamedTuple
import NamedTupleDecomposition._
import PolyfillTracking._
import TreePrettyPrinter._
import tyql.Expr.Exp

/** Logical query plan tree. Moves all type parameters into terms (using ResultTag). Collapses nested queries where
  * possible.
  */
object QueryIRTree:

  /** Wrapper around `generateQuery` that appends the Final flag to not include the surrounding ()s on the top-level.
    */
  def generateFullQuery(ast: DatabaseAST[?], symbols: SymbolTable)(using d: Dialect): RelationOp =
    generateQuery(ast, symbols).appendFlag(SelectFlags.Final) // ignore top-level parens

  /** Globally unique reference counter for identifiers, used for making aliases unique.
    */
  var idCount = 0

  /** Convert table.filter(p1).filter(p2) => table.filter(p1 && p2). Example of a heuristic tree
    * transformation/optimization
    */
  private def collapseFilters
    (filters: Seq[Expr.Fun[?, ?, ?]], comprehension: DatabaseAST[?], symbols: SymbolTable)
    : (Seq[Expr.Fun[?, ?, ?]], DatabaseAST[?]) =
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

  /** Convert n subsequent calls to flatMap into an n-way join. e.g. table.flatMap(t1 => table2.flatMap(t2 =>
    * table3.map(t3 => (k1 = t1, k2 = t2, k3 = t3))) => SELECT t1 as k1, t2 as k3, t3 as k3 FROM table1, table2, table3
    */
  private def collapseFlatMap
    (sources: Seq[RelationOp], symbols: SymbolTable, body: Any)
    (using d: Dialect)
    : (Seq[RelationOp], QueryIRNode) =
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

  private def collapseSort
    (sorts: Seq[(Expr.Fun[?, ?, ?], Ord)], comprehension: DatabaseAST[?], symbols: SymbolTable)
    : (Seq[(Expr.Fun[?, ?, ?], Ord)], DatabaseAST[?]) =
    comprehension match
      // TODO are you sure about this order? now _.sort().sort() will have different semantics than in Scala!
      case sort: Query.Sort[?, ?, ?] =>
        collapseSort(sorts :+ (sort.$body, sort.$ord), sort.$from, symbols)
      case _ => (sorts, comprehension)

  // TODO verify set vs. bag operator precendence is preserved in the generated queries
  private def collapseNaryOp(lhs: QueryIRNode, rhs: QueryIRNode, op: String, ast: DatabaseAST[?]): NaryRelationOp =
    val flattened =
      (
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

  private def collapseMap(lhs: QueryIRNode, rhs: QueryIRNode): RelationOp = ???

  private def unnest(fromNodes: Seq[RelationOp], projectIR: QueryIRNode, flatMap: DatabaseAST[?]): RelationOp =
    fromNodes
      .reduce((q1, q2) => q1.mergeWith(q2, astOther = flatMap))
      .appendProject(projectIR, astOther = flatMap)

  private def anyToExprConverter(v: Any): Expr[?, ?] =
    v match
      case _ if v.isInstanceOf[Some[?]]    => anyToExprConverter(v.asInstanceOf[Some[?]].get)
      case _ if v.isInstanceOf[Int]        => lit(v.asInstanceOf[Int])
      case _ if v.isInstanceOf[String]     => lit(v.asInstanceOf[String])
      case _ if v.isInstanceOf[Double]     => lit(v.asInstanceOf[Double])
      case _ if v.isInstanceOf[Boolean]    => lit(v.asInstanceOf[Boolean])
      case _ if v.isInstanceOf[Expr[?, ?]] => v.asInstanceOf[Expr[?, ?]]
      case _ =>
        println("RECEIVED UNEXPCTED " + v)
        assert(false)

  private[tyql] def generateUpdateToTheDB(ast: UpdateToTheDB, symbols: SymbolTable)(using d: Dialect): QueryIRNode =
    ast match
      case insertAst: Insert[?] =>
        val newValues = insertAst.values.map(_.map(anyToExprConverter).map(e => generateExpr(e, symbols)))
        InsertQueryIR(insertAst.table, insertAst.names, newValues, insertAst)
      case insertFromSelectAst: InsertFromSelect[?, ?] =>
        val selectIR = generateQuery(insertFromSelectAst.query, symbols)
        InsertFromSelectQueryIR(insertFromSelectAst.table, selectIR, insertFromSelectAst.names, insertFromSelectAst)

  /** Generate top-level or subquery
    *
    * @param ast
    *   Query AST
    * @param symbols
    *   Symbol table, e.g. list of aliases in scope
    * @return
    */
  private def generateQuery(ast: DatabaseAST[?], symbols: SymbolTable)(using d: Dialect): RelationOp =
    ast match
      case values: Query.Values[?] =>
        val listOfTuples: List[Tuple] = values.$values.toList.asInstanceOf[List[Tuple]]
        val nodes = (0 until listOfTuples.length).map(i => // XXX the usual map here crashes the compiler
          val firstTuple = listOfTuples(i)
          val firstList: List[Any] = firstTuple.toList
          val firstListOfExpr: List[Expr[?, ?]] = firstList.map(anyToExprConverter)
          val firstListOfNodes = firstListOfExpr.map(v => generateExpr(v, symbols))
          firstListOfNodes
        )
        ValuesLeaf(nodes, values.$names, values)
      case table: Table[?] =>
        TableLeaf(table.$name, table)
      case map: Query.Map[?, ?] =>
        val actualParam = generateActualParam(map.$from, map.$query.$param, symbols)
        val attrNode = generateFun(map.$query, actualParam, symbols.bind(actualParam.carriedSymbols))
        actualParam.appendProject(attrNode, map)
      case filter: Query.Filter[?, ?] =>
        val (predicateASTs, fromNodeAST) = collapseFilters(Seq(), filter, symbols)
        val actualParam = generateActualParam(fromNodeAST, filter.$pred.$param, symbols)
        val allSymbols = symbols.bind(actualParam.carriedSymbols)
        val predicateExprs = predicateASTs.map(pred =>
          generateFun(pred, actualParam, allSymbols)
        )
        val where = WhereClause(predicateExprs, filter.$pred.$body)
        actualParam.appendWhere(where, filter)
      case filter: Aggregation.AggFilter[?] => // same as for Query.Filter
        val (predicateASTs, fromNodeAST) = collapseFilters(Seq(), filter, symbols)
        val actualParam = generateActualParam(fromNodeAST, filter.$pred.$param, symbols)
        val allSymbols = symbols.bind(actualParam.carriedSymbols)
        val predicateExprs = predicateASTs.map(pred =>
          generateFun(pred, actualParam, allSymbols)
        )
        val where = WhereClause(predicateExprs, filter.$pred.$body)
        actualParam.appendWhere(where, filter)
      case sort: Query.Sort[?, ?, ?] =>
        val (orderByASTs, fromNodeAST) = collapseSort(Seq(), sort, symbols)
        val actualParam = generateActualParam(fromNodeAST, sort.$body.$param, symbols)
        val orderByExprs = orderByASTs.map(ord =>
          (generateFun(ord._1, actualParam, symbols), ord._2)
        )
        OrderedQuery(actualParam.appendFlag(SelectFlags.Final), orderByExprs, sort)
      case flatMap: Query.FlatMap[?, ?] =>
        val actualParam = generateActualParam(flatMap.$from, flatMap.$query.$param, symbols)
        val (unevaluated, boundST) =
          partiallyGenerateFun(flatMap.$query, actualParam, symbols.bind(actualParam.carriedSymbols))
        val (fromNodes, projectIR) = collapseFlatMap(
          Seq(actualParam),
          boundST,
          unevaluated
        )

        /** TODO this is where could create more complex join nodes, for now just r1.filter(f1).flatMap(a1 =>
          * r2.filter(f2).map(a2 => body(a1, a2))) => SELECT body FROM a1, a2 WHERE f1 AND f2
          */
        unnest(fromNodes, projectIR, flatMap)
      case aggFlatMap: Aggregation.AggFlatMap[?, ?] => // Separate bc AggFlatMap can contain Expr
        val actualParam = generateActualParam(aggFlatMap.$from, aggFlatMap.$query.$param, symbols)
        val (unevaluated, boundST) =
          partiallyGenerateFun(aggFlatMap.$query, actualParam, symbols.bind(actualParam.carriedSymbols))
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
      case relOp: (Query.Union[?] | Query.UnionAll[?] | Query.Intersect[?] | Query.IntersectAll[?] | Query.Except[
            ?
          ] | Query.ExceptAll[?]) =>
        val (thisN, thatN, category, op) = relOp match
          case set: Query.Union[?]        => (set.$this, set.$other, "", "UNION")
          case bag: Query.UnionAll[?]     => (bag.$this, bag.$other, " ALL", "UNION")
          case set: Query.Intersect[?]    => (set.$this, set.$other, "", "INTERSECT")
          case bag: Query.IntersectAll[?] => (bag.$this, bag.$other, " ALL", "INTERSECT")
          case set: Query.Except[?]       => (set.$this, set.$other, "", "EXCEPT")
          case bag: Query.ExceptAll[?]    => (bag.$this, bag.$other, " ALL", "EXCEPT")
        val lhs = generateQuery(thisN, symbols).appendFlag(SelectFlags.ExprLevel)
        val rhs = generateQuery(thatN, symbols).appendFlag(SelectFlags.ExprLevel)
        collapseNaryOp(lhs, rhs, s"$op$category", relOp)
      case limit: Query.Limit[?, ?] =>
        val from = generateQuery(limit.$from, symbols)
        collapseNaryOp(
          from.appendFlag(SelectFlags.Final),
          LiteralInteger(limit.$limit, Expr.IntLit(limit.$limit)),
          "LIMIT",
          limit
        )
      case offset: Query.Offset[?, ?] =>
        val from = generateQuery(offset.$from, symbols)
        collapseNaryOp(
          from.appendFlag(SelectFlags.Final),
          LiteralInteger(offset.$offset, Expr.IntLit(offset.$offset)),
          "OFFSET",
          offset
        )
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
          case q => ??? // generateQuery(q, allSymbols, multiRecursive.$resultQuery)

        MultiRecursiveRelationOp(aliases, separatedSQ, finalQ.appendFlag(SelectFlags.Final), vars, multiRecursive)

      case Query.NewGroupBy(source, grouping, sourceRefs, tags, having) =>
        val sourceIR = generateQuery(source, symbols)
        val sourceTables = sourceIR match
          case SelectQuery(_, from, _, _, _) =>
            if from.length != tags.length then throw new Exception("Unimplemented: groupBy on complex query")
            from
          case SelectAllQuery(from, _, _, _) =>
            if from.length != tags.length then throw new Exception("Unimplemented: groupBy on complex query")
            from
          case MultiRecursiveRelationOp(_, _, _, carriedSymbols, _) =>
            carriedSymbols.map(_._2)
          case _ => throw new Exception("Unimplemented: groupBy on complex query")

        val newSymbols = symbols.bind(sourceRefs.zipWithIndex.map((r, i) => (r.stringRef(), sourceTables(i))))
        // Turn off the attribute names for the grouping clause since no aliases needed
        grouping.tag match
          case t: NamedTupleTag[?, ?] => t.names = List()
          case _                      =>
        val groupingIR = generateExpr(grouping, newSymbols)
        val havingIR = having.map(h => generateExpr(h, newSymbols))

        GroupByQuery(sourceIR.appendFlag(SelectFlags.Final), groupingIR, havingIR, overrideAlias = None, ast)

      case groupBy: Query.GroupBy[?, ?, ?, ?, ?] =>
        val fromIR = generateQuery(groupBy.$source, symbols)

        def getSource(f: RelationOp): RelationOp = f match
          case SelectQuery(project, from, where, overrideAlias, ast) =>
            val select = generateFun(groupBy.$selectFn, fromIR, symbols)
            SelectQuery(collapseMap(select, project), from, where, None, groupBy) // TODO implement
          case SelectAllQuery(from, where, overrideAlias, ast) =>
            val select = generateFun(groupBy.$selectFn, fromIR, symbols)
            SelectQuery(select, from, where, None, groupBy)
          case MultiRecursiveRelationOp(aliases, query, finalQ, carriedSymbols, ast) =>
            val newSource = getSource(finalQ).appendFlags(finalQ.flags)
            MultiRecursiveRelationOp(aliases, query, newSource, carriedSymbols, ast)
          case _ =>
            val select = generateFun(groupBy.$selectFn, fromIR, symbols)
            SelectQuery(select, Seq(fromIR), Seq(), None, groupBy) // force subquery

        val source = getSource(fromIR)

        // Turn off the attribute names for the grouping clause since no aliases needed
        groupBy.$groupingFn.$body.tag match
          case t: NamedTupleTag[?, ?] => t.names = List()
          case _                      =>
        val groupByIR = generateFun(groupBy.$groupingFn, fromIR, symbols)
        val havingIR = groupBy.$havingFn.map(f => generateFun(f, fromIR, symbols))
        GroupByQuery(source.appendFlag(SelectFlags.Final), groupByIR, havingIR, overrideAlias = None, ast = groupBy)

      case _ => throw new Exception(s"Unimplemented Relation-Op AST: $ast")

  private def generateActualParam
    (from: DatabaseAST[?], formalParam: Expr.Ref[?, ?], symbols: SymbolTable)
    (using d: Dialect)
    : RelationOp =
    lookupRecursiveRef(generateQuery(from, symbols), formalParam.stringRef())

  /** Generate the actual parameter expression and bind it to the formal parameter in the symbol table, but leave the
    * function body uncompiled.
    */
  private def partiallyGenerateFun
    (fun: Expr.Fun[?, ?, ?], actualParam: RelationOp, symbols: SymbolTable)
    : (Any, SymbolTable) =
    val boundST = symbols.bind(fun.$param.stringRef(), actualParam)
    (fun.$body, boundST)

  /** Compile the function body.
    */
  private def finishGeneratingFun(funBody: Any, boundST: SymbolTable)(using d: Dialect): QueryIRNode =
    funBody match
      //      case r: Expr.Ref[?] if r.stringRef() == fun.$param.stringRef() => SelectAllExpr() // special case identity function
      case e: Expr[?, ?]     => generateExpr(e, boundST)
      case d: DatabaseAST[?] => generateQuery(d, boundST)
      case _                 => ??? // TODO: find better way to differentiate

  /** Generate a function, return the actual parameter and the function body. Sometimes, want to split this function
    * into separate steps, for the cases where you want to collate multiple function bodies within a single expression.
    */
  private def generateFun
    (fun: Expr.Fun[?, ?, ?], appliedTo: RelationOp, symbols: SymbolTable)
    (using d: Dialect)
    : QueryIRNode =
    val (body, boundSymbols) = partiallyGenerateFun(fun, appliedTo, symbols)
    finishGeneratingFun(body, boundSymbols)

  private def generateProjection
    (p: Expr.Project[?] | AggregationExpr.AggProject[?], symbols: SymbolTable)
    (using d: Dialect)
    : QueryIRNode =
    val projectAST = p match
      case e: Expr.Project[?]               => e.$a
      case a: AggregationExpr.AggProject[?] => a.$a
    val tupleOfProject =
      NamedTuple.toTuple(
        projectAST.asInstanceOf[NamedTuple[Tuple, Tuple]]
      ) // TODO: bug? See https://github.com/scala/scala3/issues/21157
    val namedTupleNames = p.tag match
      case ResultTag.NamedTupleTag(names, types) => names.lift
      case _                                     => Seq()
    val children = tupleOfProject.toList.zipWithIndex
      .map((expr, idx) =>
        val e = expr.asInstanceOf[Expr[?, ?]]
        AttrExpr(generateExpr(e, symbols), namedTupleNames(idx), e)
      )
    ProjectClause(children, p)

  private def generateExprInWindowPosition
    (ast: ExprInWindowPosition[?], symbols: SymbolTable)
    (using d: Dialect)
    : QueryIRNode =
    ast match
      case _: RowNumber => FunctionCallOp("ROW_NUMBER", Seq(), Expr.FunctionCall0[Int]("ROW_NUMBER"))
      case _: Rank      => FunctionCallOp("RANK", Seq(), Expr.FunctionCall0[Int]("RANK"))
      case _: DenseRank => FunctionCallOp("DENSE_RANK", Seq(), Expr.FunctionCall0[Int]("DENSE_RANK"))
      case n: NTile => FunctionCallOp(
          "NTILE",
          Seq(LiteralInteger(n.n, Expr.IntLit(n.n))),
          Expr.FunctionCall1[Int, Int, NonScalarExpr]("NTILE", Expr.IntLit(n.n))
        )
      case l: Lag[?, ?] =>
        if l.default.isDefined then
          FunctionCallOp(
            "LAG",
            Seq(
              generateExpr(l.e, symbols),
              LiteralInteger(l.offset.get, Expr.IntLit(l.offset.get)),
              generateExpr(l.default.get, symbols)
            ),
            null
          )
        else if l.offset.isDefined then
          FunctionCallOp(
            "LAG",
            Seq(generateExpr(l.e, symbols), LiteralInteger(l.offset.get, Expr.IntLit(l.offset.get))),
            null
          )
        else
          FunctionCallOp("LAG", Seq(generateExpr(l.e, symbols)), null)
      case l: Lead[?, ?] =>
        if l.default.isDefined then
          FunctionCallOp(
            "LEAD",
            Seq(
              generateExpr(l.e, symbols),
              LiteralInteger(l.offset.get, Expr.IntLit(l.offset.get)),
              generateExpr(l.default.get, symbols)
            ),
            null
          )
        else if l.offset.isDefined then
          FunctionCallOp(
            "LEAD",
            Seq(generateExpr(l.e, symbols), LiteralInteger(l.offset.get, Expr.IntLit(l.offset.get))),
            null
          )
        else
          FunctionCallOp("LEAD", Seq(generateExpr(l.e, symbols)), null)
      case l: FirstValue[?, ?] =>
        FunctionCallOp("FIRST_VALUE", Seq(generateExpr(l.e, symbols)), null)
      case l: LastValue[?, ?] =>
        FunctionCallOp("LAST_VALUE", Seq(generateExpr(l.e, symbols)), null)
      case l: NthValue[?, ?] =>
        FunctionCallOp("NTH_VALUE", Seq(generateExpr(l.e, symbols), LiteralInteger(l.n, Expr.IntLit(l.n))), null)

  private def generateExpr(ast: Expr[?, ?], symbols: SymbolTable)(using d: Dialect): QueryIRNode =
    ast match
      case ref: Expr.Ref[?, ?] =>
        val name = ref.stringRef()
        val sub = symbols(name)
        QueryIRVar(sub, name, ref) // TODO: singleton?
      case s: Expr.Select[?]  => SelectExpr(s.$name, generateExpr(s.$x, symbols), s)
      case p: Expr.Project[?] => generateProjection(p, symbols)
      case c: Expr.Cast[?, ?, ?] =>
        c.resultType match
          case CastTarget.CString => UnaryExprOp("CAST(", generateExpr(c.$x, symbols), s" AS ${d.stringCast})", c)
          case CastTarget.CBool   => UnaryExprOp("CAST(", generateExpr(c.$x, symbols), s" AS ${d.booleanCast})", c)
          case CastTarget.CDouble => UnaryExprOp("CAST(", generateExpr(c.$x, symbols), s" AS ${d.doubleCast})", c)
          case CastTarget.CInt    => UnaryExprOp("CAST(", generateExpr(c.$x, symbols), s" AS ${d.integerCast})", c)
          case CastTarget.CFloat  => UnaryExprOp("CAST(", generateExpr(c.$x, symbols), s" AS ${d.floatCast})", c)
          case CastTarget.CLong   => UnaryExprOp("CAST(", generateExpr(c.$x, symbols), s" AS ${d.longCast})", c)
      case w: WindExpr[?] =>
        val ae = w.ae match
          case Left(aggrExpr)    => generateExpr(aggrExpr, symbols)
          case Right(windowExpr) => generateExprInWindowPosition(windowExpr, symbols)
        val partitionBy = w.partitionBy.map(generateExpr(_, symbols))
        WindowFunctionOp(ae, partitionBy, Seq(), w)
      case g: Expr.Gt[?, ?, ?, ?] =>
        BinExprOp("", generateExpr(g.$x, symbols), " > ", generateExpr(g.$y, symbols), "", Precedence.Comparison, g)
      case g: Expr.Lt[?, ?, ?, ?] =>
        BinExprOp("", generateExpr(g.$x, symbols), " < ", generateExpr(g.$y, symbols), "", Precedence.Comparison, g)
      case g: Expr.Lte[?, ?, ?, ?] =>
        BinExprOp("", generateExpr(g.$x, symbols), " <= ", generateExpr(g.$y, symbols), "", Precedence.Comparison, g)
      case g: Expr.Gte[?, ?, ?, ?] =>
        BinExprOp("", generateExpr(g.$x, symbols), " >= ", generateExpr(g.$y, symbols), "", Precedence.Comparison, g)
      case b: Expr.Between[?, ?, ?, ?] =>
        val expr = generateExpr(b.$x, symbols)
        val lower = generateExpr(b.$min, symbols)
        val upper = generateExpr(b.$max, symbols)
        val ae = ("ae", Precedence.Comparison)
        val al = ("al", Precedence.Comparison)
        val au = ("au", Precedence.Comparison)
        RawSQLInsertOp(
          SqlSnippet(Precedence.Comparison, snippet"$ae BETWEEN $al AND $au"),
          Map(ae._1 -> expr, al._1 -> lower, au._1 -> upper),
          Precedence.Comparison,
          b
        )
      case a: Expr.And[?, ?] =>
        BinExprOp("", generateExpr(a.$x, symbols), " AND ", generateExpr(a.$y, symbols), "", Precedence.And, a)
      case a: Expr.Or[?, ?] =>
        BinExprOp("", generateExpr(a.$x, symbols), " OR ", generateExpr(a.$y, symbols), "", Precedence.Or, a)
      case Expr.Not(inner: Expr.IsNull[?, ?]) => UnaryExprOp("", generateExpr(inner.$x, symbols), " IS NOT NULL", ast)
      case n: Expr.Not[?]                     => UnaryExprOp("NOT ", generateExpr(n.$x, symbols), "", n)
      case x: Expr.Xor[?] =>
        d.xorOperatorSupportedNatively match
          case true =>
            BinExprOp("", generateExpr(x.$x, symbols), " XOR ", generateExpr(x.$y, symbols), "", 45, x)
          case false =>
            polyfillWasUsed()
            BinExprOp("(", generateExpr(x.$x, symbols), " = TRUE) <> (", generateExpr(x.$y, symbols), " = TRUE)", 43, x)
      case f0: Expr.FunctionCall0[?]       => FunctionCallOp(f0.name, Seq(), f0)
      case f1: Expr.FunctionCall1[?, ?, ?] => FunctionCallOp(f1.name, Seq(generateExpr(f1.$a1, symbols)), f1)
      case f2: Expr.FunctionCall2[?, ?, ?, ?, ?] =>
        FunctionCallOp(f2.name, Seq(f2.$a1, f2.$a1).map(generateExpr(_, symbols)), f2)
      case u: Expr.RandomUUID => FunctionCallOp(d.feature_RandomUUID_functionName, Seq(), u)
      case f: Expr.RandomFloat =>
        assert(
          d.feature_RandomFloat_functionName.isDefined != d.feature_RandomFloat_rawSQL.isDefined,
          "RandomFloat dialect feature must have either a function name or raw SQL"
        )
        if d.feature_RandomFloat_functionName.isDefined then
          FunctionCallOp(d.feature_RandomFloat_functionName.get, Seq(), f)
        else
          polyfillWasUsed()
          RawSQLInsertOp(d.feature_RandomFloat_rawSQL.get, Map(), d.feature_RandomFloat_rawSQL.get.precedence, f)
      case i: Expr.RandomInt[?, ?] =>
        polyfillWasUsed()
        RawSQLInsertOp(
          d.feature_RandomInt_rawSQL,
          Map("a" -> generateExpr(i.$x, symbols), "b" -> generateExpr(i.$y, symbols)),
          d.feature_RandomInt_rawSQL.precedence,
          i
        )
      case a: Expr.Plus[?, ?, ?] =>
        BinExprOp("", generateExpr(a.$x, symbols), " + ", generateExpr(a.$y, symbols), "", Precedence.Additive, a)
      case a: Expr.Minus[?, ?, ?] =>
        BinExprOp("", generateExpr(a.$x, symbols), " - ", generateExpr(a.$y, symbols), "", Precedence.Additive, a)
      case a: Expr.Times[?, ?, ?] =>
        BinExprOp("", generateExpr(a.$x, symbols), " * ", generateExpr(a.$y, symbols), "", Precedence.Multiplicative, a)
      case a: Expr.Eq[?, ?] =>
        BinExprOp("", generateExpr(a.$x, symbols), " = ", generateExpr(a.$y, symbols), "", Precedence.Comparison, a)
      case a: Expr.Ne[?, ?] =>
        BinExprOp("", generateExpr(a.$x, symbols), " <> ", generateExpr(a.$y, symbols), "", Precedence.Comparison, a)
      case e: Expr.NullSafeEq[?, ?] =>
        val a = ("a", Precedence.Comparison)
        val b = ("b", Precedence.Comparison)
        RawSQLInsertOp(
          SqlSnippet(
            Precedence.Comparison,
            if d.nullSafeEqualityViaSpecialOperator then
              snippet"$a <=> $b"
            else
              snippet"$a IS NOT DISTINCT FROM $b"
          ),
          Map(a._1 -> generateExpr(e.$x, symbols), b._1 -> generateExpr(e.$y, symbols)),
          Precedence.Comparison,
          e
        )
      case e: Expr.NullSafeNe[?, ?] =>
        val a = ("a", Precedence.Comparison)
        val b = ("b", Precedence.Comparison)
        if d.nullSafeEqualityViaSpecialOperator then
          RawSQLInsertOp(
            SqlSnippet(Precedence.Unary, snippet"NOT($a <=> $b)"),
            Map(a._1 -> generateExpr(e.$x, symbols), b._1 -> generateExpr(e.$y, symbols)),
            Precedence.Unary,
            e
          )
        else
          RawSQLInsertOp(
            SqlSnippet(Precedence.Comparison, snippet"$a IS DISTINCT FROM $b"),
            Map(a._1 -> generateExpr(e.$x, symbols), b._1 -> generateExpr(e.$y, symbols)),
            Precedence.Comparison,
            e
          )
      case a: Expr.Modulo[?, ?] =>
        BinExprOp("", generateExpr(a.$x, symbols), " % ", generateExpr(a.$y, symbols), "", Precedence.Multiplicative, a)
      case r: Expr.Round[?, ?] => FunctionCallOp("ROUND", Seq(generateExpr(r.$x, symbols)), r)
      case r: Expr.RoundWithPrecision[?, ?, ?] =>
        FunctionCallOp("ROUND", Seq(generateExpr(r.$x, symbols), generateExpr(r.$precision, symbols)), r)
      case c: Expr.Ceil[?, ?]  => FunctionCallOp("CEIL", Seq(generateExpr(c.$x, symbols)), c)
      case f: Expr.Floor[?, ?] => FunctionCallOp("FLOOR", Seq(generateExpr(f.$x, symbols)), f)
      case p: Expr.Power[?, ?, ?, ?] =>
        FunctionCallOp("POWER", Seq(generateExpr(p.$x, symbols), generateExpr(p.$y, symbols)), p)
      case s: Expr.Sqrt[?, ?]       => FunctionCallOp("SQRT", Seq(generateExpr(s.$x, symbols)), s)
      case s: Expr.Sign[?, ?]       => FunctionCallOp("SIGN", Seq(generateExpr(s.$x, symbols)), s)
      case l: Expr.LogNatural[?, ?] => FunctionCallOp("LN", Seq(generateExpr(l.$x, symbols)), l)
      case l: Expr.Log[?, ?, ?, ?] =>
        FunctionCallOp("LOG", Seq(generateExpr(l.$base, symbols), generateExpr(l.$x, symbols)), l)
      case e: Expr.Exp[?, ?]  => FunctionCallOp("EXP", Seq(generateExpr(e.$x, symbols)), e)
      case s: Expr.Sin[?, ?]  => FunctionCallOp("SIN", Seq(generateExpr(s.$x, symbols)), s)
      case s: Expr.Cos[?, ?]  => FunctionCallOp("COS", Seq(generateExpr(s.$x, symbols)), s)
      case s: Expr.Tan[?, ?]  => FunctionCallOp("TAN", Seq(generateExpr(s.$x, symbols)), s)
      case s: Expr.Asin[?, ?] => FunctionCallOp("ASIN", Seq(generateExpr(s.$x, symbols)), s)
      case s: Expr.Acos[?, ?] => FunctionCallOp("ACOS", Seq(generateExpr(s.$x, symbols)), s)
      case s: Expr.Atan[?, ?] => FunctionCallOp("ATAN", Seq(generateExpr(s.$x, symbols)), s)
      case a: Expr.Concat[?, ?, ?, ?] =>
        val lhsIR = generateExpr(a.$x, symbols) match
          case p: ProjectClause => p
          case v: QueryIRVar    => SelectExpr("*", v, a.$x)
          case _ => throw new Exception("Unimplemented: concatting something that is not a literal nor a variable")
        val rhsIR = generateExpr(a.$y, symbols) match
          case p: ProjectClause => p
          case v: QueryIRVar    => SelectExpr("*", v, a.$y)
          case _ => throw new Exception("Unimplemented: concatting something that is not a literal nor a variable")

        BinExprOp(
          "",
          lhsIR,
          ", ",
          rhsIR,
          "",
          Precedence.Concat,
          a
        )
      case n: Expr.NullLit[?] => RawSQLInsertOp(SqlSnippet(Precedence.Unary, snippet"NULL"), Map(), Precedence.Unary, n)
      case i: Expr.IsNull[?, ?] => UnaryExprOp("", generateExpr(i.$x, symbols), " IS NULL", i)
      case c: Expr.Coalesce[?, ?] =>
        FunctionCallOp("COALESCE", (Seq(c.$x1, c.$x2) ++ c.$xs).map(generateExpr(_, symbols)), c)
      case i: Expr.NullIf[?, ?, ?] =>
        FunctionCallOp("NULLIF", Seq(generateExpr(i.$x, symbols), generateExpr(i.$y, symbols)), i)
      case l: Expr.DoubleLit => LiteralDouble(l.$value, l)
      case l: Expr.IntLit    => LiteralInteger(l.$value, l)
      case l: Expr.StringLit => LiteralString(l.$value, insideLikePatternQuoting = false, l)
      case l: Expr.BooleanLit =>
        if l.$value then
          RawSQLInsertOp(SqlSnippet(Precedence.Unary, snippet"TRUE"), Map(), Precedence.Unary, l)
        else
          RawSQLInsertOp(SqlSnippet(Precedence.Unary, snippet"FALSE"), Map(), Precedence.Unary, l)
      case c: Expr.SearchedCase[?, ?, ?] => SearchedCaseOp(
          c.$cases.map(w => (generateExpr(w._1, symbols), generateExpr(w._2, symbols))),
          c.$else.map(generateExpr(_, symbols)),
          c
        )
      case c: Expr.SimpleCase[?, ?, ?, ?] => SimpleCaseOp(
          generateExpr(c.$expr, symbols),
          c.$cases.map(w => (generateExpr(w._1, symbols), generateExpr(w._2, symbols))),
          c.$else.map(generateExpr(_, symbols)),
          c
        )
      case l: Expr.Lower[?]      => UnaryExprOp("LOWER(", generateExpr(l.$x, symbols), ")", l)
      case l: Expr.Upper[?]      => UnaryExprOp("UPPER(", generateExpr(l.$x, symbols), ")", l)
      case l: Expr.StrReverse[?] => UnaryExprOp("REVERSE(", generateExpr(l.$x, symbols), ")", l)
      case l: Expr.Trim[?]       => UnaryExprOp("TRIM(", generateExpr(l.$x, symbols), ")", l)
      case l: Expr.LTrim[?]      => UnaryExprOp("LTRIM(", generateExpr(l.$x, symbols), ")", l)
      case l: Expr.RTrim[?]      => UnaryExprOp("RTRIM(", generateExpr(l.$x, symbols), ")", l)
      case l: Expr.StrReplace[?, ?] => FunctionCallOp(
          "REPLACE",
          Seq(generateExpr(l.$s, symbols), generateExpr(l.$from, symbols), generateExpr(l.$to, symbols)),
          l
        )
      case l: Expr.StrLike[?, ?] =>
        l.$pattern match
          case Expr.StringLit(pattern) => BinExprOp(
              "",
              generateExpr(l.$s, symbols),
              "LIKE ",
              LiteralString(pattern, insideLikePatternQuoting = true, l.$pattern),
              "",
              Precedence.Comparison,
              l
            )
          case _ => assert(false, "LIKE pattern must be a string literal")
      case l: Expr.Substring[?, ?] =>
        if l.$len.isEmpty then
          FunctionCallOp("SUBSTRING", Seq(generateExpr(l.$s, symbols), generateExpr(l.$from, symbols)), l)
        else
          FunctionCallOp(
            "SUBSTRING",
            Seq(generateExpr(l.$s, symbols), generateExpr(l.$from, symbols), generateExpr(l.$len.get, symbols)),
            l
          )
      case l: Expr.StrConcat[?, ?] => FunctionCallOp("CONCAT", (Seq(l.$x) ++ l.$xs).map(generateExpr(_, symbols)), l)
      case l: Expr.StrConcatUniform[?] =>
        FunctionCallOp("CONCAT", (Seq(l.$x) ++ l.$xs).map(generateExpr(_, symbols)), l)
      case l: Expr.StrConcatSeparator[?, ?] =>
        FunctionCallOp("CONCAT_WS", (Seq(l.$sep, l.$x) ++ l.$xs).map(generateExpr(_, symbols)), l)
      case l: Expr.StringCharLength[?] =>
        UnaryExprOp(d.stringLengthByCharacters + "(", generateExpr(l.$x, symbols), ")", l)
      case l: Expr.StringByteLength[?] =>
        if d.stringLengthBytesNeedsEncodeFirst then
          UnaryExprOp("OCTET_LENGTH(ENCODE(", generateExpr(l.$x, symbols), "))", l)
        else
          UnaryExprOp("OCTET_LENGTH(", generateExpr(l.$x, symbols), ")", l)
      case l: Expr.StrRepeat[?, ?] =>
        if !d.needsStringRepeatPolyfill then
          FunctionCallOp("REPEAT", Seq(generateExpr(l.$s, symbols), generateExpr(l.$n, symbols)), l)
        else
          val str = ("str", Precedence.Concat)
          val num = ("num", Precedence.Concat)
          polyfillWasUsed()
          RawSQLInsertOp(
            SqlSnippet(
              Precedence.Unary,
              snippet"(with stringRepeatParameters as (select $str as str, $num as num) select SUBSTR(REPLACE(PRINTF('%.*c', num, 'x'), 'x', str), 1, length(str)*num) from stringRepeatParameters)"
            ),
            Map(str._1 -> generateExpr(l.$s, symbols), num._1 -> generateExpr(l.$n, symbols)),
            Precedence.Unary,
            l
          )
      case l: Expr.StrLPad[?, ?] =>
        if !d.needsStringLPadRPadPolyfill then
          FunctionCallOp(
            "LPAD",
            Seq(generateExpr(l.$s, symbols), generateExpr(l.$len, symbols), generateExpr(l.$pad, symbols)),
            l
          )
        else
          val str = ("str", Precedence.Concat)
          val pad = ("pad", Precedence.Concat)
          val num = ("num", Precedence.Concat)
          polyfillWasUsed()
          RawSQLInsertOp(
            SqlSnippet(
              Precedence.Unary,
              snippet"(with lpadParameters as (select $str as str, $pad as pad, $num as num) select substr(substr(replace(hex(zeroblob(num)), '00', pad), 1, num - length(str)) || str, 1, num) from lpadParameters)"
            ),
            Map(
              str._1 -> generateExpr(l.$s, symbols),
              pad._1 -> generateExpr(l.$pad, symbols),
              num._1 -> generateExpr(l.$len, symbols)
            ),
            Precedence.Unary,
            l
          )
      case l: Expr.StrRPad[?, ?] =>
        if !d.needsStringLPadRPadPolyfill then
          FunctionCallOp(
            "RPAD",
            Seq(generateExpr(l.$s, symbols), generateExpr(l.$len, symbols), generateExpr(l.$pad, symbols)),
            l
          )
        else
          val str = ("str", Precedence.Concat)
          val pad = ("pad", Precedence.Concat)
          val num = ("num", Precedence.Concat)
          polyfillWasUsed()
          RawSQLInsertOp(
            SqlSnippet(
              Precedence.Unary,
              snippet"(with rpadParameters as (select $str as str, $pad as pad, $num as num) select substr(str || substr(replace(hex(zeroblob(num)), '00', pad), 1, num - length(str)), 1, num) from rpadParameters)"
            ),
            Map(
              str._1 -> generateExpr(l.$s, symbols),
              pad._1 -> generateExpr(l.$pad, symbols),
              num._1 -> generateExpr(l.$len, symbols)
            ),
            Precedence.Unary,
            l
          )
      case l: Expr.StrPositionIn[?, ?] => d.stringPositionFindingVia match
          case "POSITION" => BinExprOp(
              "POSITION(",
              generateExpr(l.$substr, symbols),
              " IN ",
              generateExpr(l.$string, symbols),
              ")",
              Precedence.Unary,
              l
            )
          case "LOCATE" =>
            FunctionCallOp("LOCATE", Seq(generateExpr(l.$substr, symbols), generateExpr(l.$string, symbols)), l)
          case "INSTR" =>
            FunctionCallOp("INSTR", Seq(generateExpr(l.$string, symbols), generateExpr(l.$substr, symbols)), l)
      case a: AggregationExpr[?]  => generateAggregation(a, symbols)
      case a: Aggregation[?, ?]   => generateQuery(a, symbols).appendFlag(SelectFlags.ExprLevel)
      case list: Expr.ListExpr[?] => ListTypeExpr(list.$elements.map(generateExpr(_, symbols)), list)
      case p: Expr.ListPrepend[?] => BinExprOp(
          "list_prepend(",
          generateExpr(p.$x, symbols),
          ", ",
          generateExpr(p.$list, symbols),
          ")",
          Precedence.Unary,
          p
        )
      case p: Expr.ListAppend[?] => BinExprOp(
          "list_append(",
          generateExpr(p.$list, symbols),
          ", ",
          generateExpr(p.$x, symbols),
          ")",
          Precedence.Unary,
          p
        )
      case p: Expr.ListContains[?] => BinExprOp(
          "list_contains(",
          generateExpr(p.$list, symbols),
          ", ",
          generateExpr(p.$x, symbols),
          ")",
          Precedence.Unary,
          p
        )
      case p: Expr.ListLength[?] => UnaryExprOp("length(", generateExpr(p.$list, symbols), ")", p)
      case p: Expr.NonEmpty[?] =>
        UnaryExprOp("EXISTS (", generateQuery(p.$this, symbols).appendFlag(SelectFlags.Final), ")", p)
      case p: Expr.IsEmpty[?] =>
        UnaryExprOp("NOT EXISTS (", generateQuery(p.$this, symbols).appendFlag(SelectFlags.Final), ")", p)
      case p: Expr.Contains[?] =>
        val inner = generateExpr(p.$expr, symbols)
        val needsParens = !isLiteral(inner)
        BinExprOp(
          if needsParens then "(" else "",
          removeAliases(inner),
          (if needsParens then ")" else "") + " IN (",
          generateQuery(p.$query, symbols).appendFlag(SelectFlags.Final),
          ")",
          Precedence.Comparison,
          p
        )
      case _ => throw new Exception(s"Unimplemented Expr AST: $ast")

  private def generateAggregation(ast: AggregationExpr[?], symbols: SymbolTable)(using d: Dialect): QueryIRNode =
    ast match
      case s: AggregationExpr.Sum[?]        => UnaryExprOp("SUM(", generateExpr(s.$a, symbols), ")", s)
      case s: AggregationExpr.Avg[?]        => UnaryExprOp("AVG(", generateExpr(s.$a, symbols), ")", s)
      case s: AggregationExpr.Min[?]        => UnaryExprOp("MIN(", generateExpr(s.$a, symbols), ")", s)
      case s: AggregationExpr.Max[?]        => UnaryExprOp("MAX(", generateExpr(s.$a, symbols), ")", s)
      case c: AggregationExpr.Count[?]      => UnaryExprOp("COUNT(", generateExpr(c.$a, symbols), ")", c)
      case p: AggregationExpr.AggProject[?] => generateProjection(p, symbols)

  private def removeAliases(x: QueryIRNode): QueryIRNode =
    x match
      case ProjectClause(children, ast) => ProjectClause(children.map(removeAliases), ast)
      case AttrExpr(child, name, ast)   => AttrExpr(child, None, ast)
      case _                            => x

  private def isLiteral(x: QueryIRNode): Boolean =
    x match
      case LiteralInteger(_, _)   => true
      case LiteralDouble(_, _)    => true
      case LiteralString(_, _, _) => true
      case _                      => false
