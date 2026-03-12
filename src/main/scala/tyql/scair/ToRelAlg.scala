package tyql

import scair.Printer
import scair.ir.*
import scair.dialects.builtin.*
import scair.dialects.func.{Func, Return}
import scair.dialects.relalg.{
  BaseTable, Selection, MapOp, InnerJoin, CrossProduct,
  Aggregation as RelAlgAggregation, AggrFn, CountRows, Sort, Limit,
  Projection, Materialize, RelAlgQuery, QueryReturn,
  AggrFunc, SetSemantic, SortSpec, SortSpecificationAttr,
}
import scair.dialects.db.*
import scair.dialects.tuples.*
import scair.dialects.subop.{SubopLocalTableType, LocalTableColumn, SetResult}

import java.io.{PrintWriter, StringWriter}
import java.time.LocalDate

/** Schema information for a table's columns. */
case class TableSchema(
    tableName: String,
    columns: Map[String, Attribute] // colName -> MLIR type attribute
)

/** Converts a tyql QueryIRNode (RelationOp tree) to relalg MLIR using scair.
  *
  * Usage:
  * {{{
  *   val gen = RelAlgGenerator(schemas)
  *   val mlir = gen.convert(queryIR)
  * }}}
  */
class RelAlgGenerator(schemas: Map[String, TableSchema]):

  private val ts = TupleStreamType()
  private val tupleT = TupleType()

  // ── Helpers for column references ──

  private def mkColRef(tableName: String, colName: String): ColumnRefAttr =
    ColumnRefAttr(StringData(tableName), StringData(colName))

  private def mkColDef(
      tableName: String,
      colName: String,
      typ: Attribute,
  ): ColumnDefAttr =
    ColumnDefAttr(StringData(tableName), StringData(colName), typ)

  /** Convert a tyql IR tree to an MLIR string. */
  def convert(ir: QueryIRNode): String =
    val ops = scala.collection.mutable.ArrayBuffer[Operation]()
    val resultVal = emitRelation(ir.asInstanceOf[RelationOp], ops)
    val module =
      ModuleOp(body = Region(Seq(Block(operations = ops.toSeq))))
    val out = StringWriter()
    Printer(p = PrintWriter(out)).print(module)(using 0)
    out.toString().trim()

  /** Convert a tyql IR tree to executable MLIR wrapped in func.func @main().
    * The wrapper adds: relalg.query, relalg.materialize, relalg.query_return,
    * subop.set_result, and func.return — making the output runnable by LingoDB's run-mlir.
    */
  def convertExecutable(ir: QueryIRNode, outputColumns: Seq[RelAlgGenerator.OutputColumn]): String =
    val queryBodyOps = scala.collection.mutable.ArrayBuffer[Operation]()
    val lastRelVal = emitRelation(ir.asInstanceOf[RelationOp], queryBodyOps)

    // Build the SubopLocalTableType from output columns
    val localTableCols = outputColumns.map { oc =>
      LocalTableColumn(StringData(oc.colName), oc.colType)
    }
    val localTableNames = outputColumns.map(oc => StringData(oc.outputName))
    val localTableType = SubopLocalTableType(localTableCols, localTableNames)

    // Materialize
    val matCols = outputColumns.map(oc =>
      mkColRef(oc.scope, oc.colName)
    )
    val matNames = outputColumns.map(oc => StringData(oc.outputName))
    val matOp = Materialize(
      rel = lastRelVal.asInstanceOf[Value[TupleStreamType]],
      cols = ArrayAttribute(matCols),
      colNames = ArrayAttribute(matNames),
      result = Result(localTableType),
    )
    queryBodyOps += matOp

    // QueryReturn (terminator)
    val qret = QueryReturn(rel = matOp.result.asInstanceOf[Value[Attribute]])
    queryBodyOps += qret

    // RelAlgQuery wrapping the body
    val queryOp = RelAlgQuery(
      body = Region(Seq(Block(operations = queryBodyOps.toSeq))),
      result = Result(localTableType),
    )

    // subop.set_result 0 %query_result : type
    val setResult = SetResult(
      index = IntData(0),
      rel = queryOp.result.asInstanceOf[Value[Attribute]],
    )

    // func.return
    val funcReturn = Return(Seq())

    // func.func @main() { ... }
    val funcOp = Func(
      sym_name = StringData("main"),
      function_type = FunctionType(inputs = Seq(), outputs = Seq()),
      sym_visibility = None,
      body = Region(Seq(Block(operations = Seq(queryOp, setResult, funcReturn)))),
    )

    // module { func.func @main() { ... } }
    val module = ModuleOp(body = Region(Seq(Block(operations = Seq(funcOp)))))
    val out = StringWriter()
    Printer(p = PrintWriter(out)).print(module)(using 0)
    out.toString().trim()

  // ── Relation-level translation ──

  private def emitRelation(
      op: RelationOp,
      ops: scala.collection.mutable.ArrayBuffer[Operation],
  ): Value[Attribute] =
    op match
      case t: TableLeaf      => emitBaseTable(t, ops)
      case s: SelectAllQuery => emitSelectAll(s, ops)
      case s: SelectQuery    => emitSelect(s, ops)
      case o: OrderedQuery   => emitOrdered(o, ops)
      case g: GroupByQuery   => emitGroupBy(g, ops)
      case n: NaryRelationOp => emitNary(n, ops)
      case _ =>
        throw Exception(
          s"Unsupported RelationOp: ${op.getClass.getSimpleName}"
        )

  private def emitBaseTable(
      t: TableLeaf,
      ops: scala.collection.mutable.ArrayBuffer[Operation],
  ): Value[Attribute] =
    val schema = schemas.getOrElse(
      t.tableName,
      throw Exception(s"No schema for table '${t.tableName}'"),
    )
    val columnEntries = schema.columns.map { case (colName, colType) =>
      colName -> mkColDef(t.tableName, colName, colType).asInstanceOf[Attribute]
    }
    val bt = BaseTable(
      tableId = StringData(t.tableName),
      columns = DictionaryAttr(columnEntries),
      result = Result(ts),
    )
    ops += bt
    bt.result

  private def emitSelectAll(
      s: SelectAllQuery,
      ops: scala.collection.mutable.ArrayBuffer[Operation],
  ): Value[Attribute] =
    val fromVal = emitFrom(s.from, ops)
    if s.where.nonEmpty then emitSelection(fromVal, s.where, ops)
    else fromVal

  private def emitSelect(
      s: SelectQuery,
      ops: scala.collection.mutable.ArrayBuffer[Operation],
  ): Value[Attribute] =
    val fromVal = emitFrom(s.from, ops)
    val filtered =
      if s.where.nonEmpty then emitSelection(fromVal, s.where, ops)
      else fromVal

    val projNodes = s.project match
      case pc: ProjectClause => pc.children
      case other             => Seq(other)

    val hasAggregation = projNodes.exists(containsAggregation)

    if hasAggregation then
      emitAggregationFromProjection(filtered, projNodes, ops)
    else emitMapFromProjection(filtered, projNodes, ops)

  private def emitOrdered(
      o: OrderedQuery,
      ops: scala.collection.mutable.ArrayBuffer[Operation],
  ): Value[Attribute] =
    val innerVal = emitRelation(o.query, ops)
    val sortSpecs = o.sortFn.map { case (node, ord) =>
      val (tableName, colName) = extractColumnRef(node)
      val spec = ord match
        case Ord.ASC  => SortSpec.asc
        case Ord.DESC => SortSpec.desc
      SortSpecificationAttr(mkColRef(tableName, colName), spec)
    }
    val sortOp = Sort(
      rel = innerVal.asInstanceOf[Value[TupleStreamType]],
      sortspecs = ArrayAttribute(sortSpecs),
      result = Result(ts),
    )
    ops += sortOp
    sortOp.result

  private def emitGroupBy(
      g: GroupByQuery,
      ops: scala.collection.mutable.ArrayBuffer[Operation],
  ): Value[Attribute] =
    val sourceOp = g.source match
      case sq: SelectQuery =>
        val fromVal = emitFrom(sq.from, ops)
        val filtered =
          if sq.where.nonEmpty then emitSelection(fromVal, sq.where, ops)
          else fromVal

        val groupByColRefs = extractGroupByColumns(g.groupBy)
        val projNodes = sq.project match
          case pc: ProjectClause => pc.children
          case other             => Seq(other)

        emitAggregationOp(filtered, groupByColRefs, projNodes, ops)

      case other =>
        val innerVal = emitRelation(other, ops)
        val groupByColRefs = extractGroupByColumns(g.groupBy)
        emitAggregationOp(innerVal, groupByColRefs, Seq(), ops)

    sourceOp

  private def emitNary(
      n: NaryRelationOp,
      ops: scala.collection.mutable.ArrayBuffer[Operation],
  ): Value[Attribute] =
    n.op match
      case "LIMIT" =>
        val innerNode = n.children.head
        val limitNode = n.children(1)
        val innerVal = innerNode match
          case r: RelationOp => emitRelation(r, ops)
          case _ =>
            throw Exception(
              s"Expected RelationOp in LIMIT, got ${innerNode.getClass}"
            )
        val maxRows = limitNode match
          case lit: Literal => lit.stringRep.toInt
          case _ => throw Exception(s"Expected Literal for LIMIT count")
        val limitOp = Limit(
          rel = innerVal.asInstanceOf[Value[TupleStreamType]],
          maxRows = IntData(maxRows),
          result = Result(ts),
        )
        ops += limitOp
        limitOp.result
      case _ =>
        throw Exception(s"Unsupported NaryRelationOp: ${n.op}")

  // ── Helpers ──

  private def emitFrom(
      from: Seq[RelationOp],
      ops: scala.collection.mutable.ArrayBuffer[Operation],
  ): Value[Attribute] =
    val vals = from.map(emitRelation(_, ops))
    vals.reduceLeft { (left, right) =>
      val cp = CrossProduct(
        left = left.asInstanceOf[Value[TupleStreamType]],
        right = right.asInstanceOf[Value[TupleStreamType]],
        result = Result(ts),
      )
      ops += cp
      cp.result
    }

  private def emitSelection(
      rel: Value[Attribute],
      where: Seq[QueryIRNode],
      ops: scala.collection.mutable.ArrayBuffer[Operation],
  ): Value[Attribute] =
    val selOp = Selection(
      rel = rel.asInstanceOf[Value[TupleStreamType]],
      predicate = Region(Seq(
        Block(
          Seq(tupleT),
          (args: Iterable[Value[Attribute]]) =>
            val tupleArg = args.head
            val regionOps =
              scala.collection.mutable.ArrayBuffer[Operation]()

            val atomicPreds = where.flatMap(flattenAllAnds)
            val predVals = atomicPreds.map(emitPredicate(_, tupleArg, regionOps))

            val finalPred =
              if predVals.size == 1 then predVals.head
              else
                val andOp = DBAnd(
                  vals = predVals.map(_._1),
                  result = Result(I1),
                )
                regionOps += andOp
                (andOp.result.asInstanceOf[Value[Attribute]], "i1")

            val ret = TuplesReturn(results_ = Seq(finalPred._1))
            regionOps += ret
            regionOps.toSeq,
        )
      )),
      result = Result(ts),
    )
    ops += selOp
    selOp.result

  /** Emit an expression inside a predicate region. Returns (value,
    * typeString).
    */
  private def emitPredicate(
      node: QueryIRNode,
      tupleArg: Value[Attribute],
      ops: scala.collection.mutable.ArrayBuffer[Operation],
  ): (Value[Attribute], String) =
    node match
      case bin: BinExprOp =>
        bin.ast match
          // Logical AND — flatten
          case _: Expr.And =>
            val leftPreds = flattenAnd(bin.lhs)
            val rightPreds = flattenAnd(bin.rhs)
            val allPreds =
              (leftPreds ++ rightPreds).map(emitPredicate(_, tupleArg, ops))
            if allPreds.size == 1 then allPreds.head
            else
              val andOp = DBAnd(
                vals = allPreds.map(_._1),
                result = Result(I1),
              )
              ops += andOp
              (andOp.result.asInstanceOf[Value[Attribute]], "i1")

          // Comparisons
          case _: (Expr.Gt | Expr.GtDouble | Expr.GtDate) =>
            emitCompare(CmpPredicate.gt, bin.lhs, bin.rhs, tupleArg, ops)
          case _: (Expr.Gte | Expr.GteDouble | Expr.GteDate) =>
            emitCompare(CmpPredicate.gte, bin.lhs, bin.rhs, tupleArg, ops)
          case _: (Expr.Lt | Expr.LtDouble | Expr.LtDate) =>
            emitCompare(CmpPredicate.lt, bin.lhs, bin.rhs, tupleArg, ops)
          case _: (Expr.Lte | Expr.LteDouble | Expr.LteDate) =>
            emitCompare(CmpPredicate.lte, bin.lhs, bin.rhs, tupleArg, ops)
          case _: Expr.Eq =>
            emitCompare(CmpPredicate.eq, bin.lhs, bin.rhs, tupleArg, ops)
          case _: Expr.Ne =>
            emitCompare(CmpPredicate.neq, bin.lhs, bin.rhs, tupleArg, ops)

          // Arithmetic
          case _: Expr.Plus[?] =>
            emitArith("db.add", bin.lhs, bin.rhs, tupleArg, ops)
          case _: Expr.Minus[?] =>
            emitArith("db.sub", bin.lhs, bin.rhs, tupleArg, ops)
          case _: Expr.Times[?] =>
            emitArith("db.mul", bin.lhs, bin.rhs, tupleArg, ops)

          case _ =>
            throw Exception(
              s"Unsupported BinExprOp ast: ${bin.ast.getClass.getSimpleName}"
            )

      case unary: UnaryExprOp =>
        unary.ast match
          case _: Expr.Not =>
            val (childVal, childTy) =
              emitPredicate(unary.child, tupleArg, ops)
            val notOp = DBNot(
              val_ = childVal,
              result = Result(I1),
            )
            ops += notOp
            (notOp.result.asInstanceOf[Value[Attribute]], "i1")
          case _ =>
            throw Exception(
              s"Unsupported UnaryExprOp in predicate: ${unary.ast.getClass.getSimpleName}"
            )

      case sel: SelectExpr =>
        emitGetCol(sel, tupleArg, ops)

      case lit: Literal =>
        emitConstant(lit, ops)

      case v: QueryIRVar =>
        throw Exception(s"Unexpected QueryIRVar in predicate context")

      case _ =>
        throw Exception(
          s"Unsupported predicate node: ${node.getClass.getSimpleName}"
        )

  private def emitExpr(
      node: QueryIRNode,
      tupleArg: Value[Attribute],
      ops: scala.collection.mutable.ArrayBuffer[Operation],
  ): (Value[Attribute], String) =
    emitPredicate(node, tupleArg, ops)

  private def emitCompare(
      pred: CmpPredicate,
      lhs: QueryIRNode,
      rhs: QueryIRNode,
      tupleArg: Value[Attribute],
      ops: scala.collection.mutable.ArrayBuffer[Operation],
  ): (Value[Attribute], String) =
    val (lVal0, lTy) = emitPredicate(lhs, tupleArg, ops)
    val (rVal0, rTy) = emitPredicate(rhs, tupleArg, ops)
    // Auto-cast char<N> to string when comparing with string
    val (lVal, rVal) = (lVal0.typ, rVal0.typ) match
      case (_: CharType, _: DBStringType) =>
        val cast = DBCast(val_ = lVal0, result = Result(DBStringType()))
        ops += cast
        (cast.result.asInstanceOf[Value[Attribute]], rVal0)
      case (_: DBStringType, _: CharType) =>
        val cast = DBCast(val_ = rVal0, result = Result(DBStringType()))
        ops += cast
        (lVal0, cast.result.asInstanceOf[Value[Attribute]])
      case _ => (lVal0, rVal0)
    val cmp = DBCompare(
      predicate = pred,
      lhs = lVal,
      rhs = rVal,
      result = Result(I1),
    )
    ops += cmp
    (cmp.result.asInstanceOf[Value[Attribute]], "i1")

  private def emitArith(
      opName: String,
      lhs: QueryIRNode,
      rhs: QueryIRNode,
      tupleArg: Value[Attribute],
      ops: scala.collection.mutable.ArrayBuffer[Operation],
  ): (Value[Attribute], String) =
    val (lVal, lTy) = emitPredicate(lhs, tupleArg, ops)
    val (rVal, rTy) = emitPredicate(rhs, tupleArg, ops)
    val resultType = computeArithResultType(opName, lVal.typ, rVal.typ)
    val arithOp = opName match
      case "db.add" => DBAdd(lhs = lVal, rhs = rVal, result = Result(resultType))
      case "db.sub" => DBSub(lhs = lVal, rhs = rVal, result = Result(resultType))
      case "db.mul" => DBMul(lhs = lVal, rhs = rVal, result = Result(resultType))
      case "db.div" => DBDiv(lhs = lVal, rhs = rVal, result = Result(resultType))
      case _ => throw Exception(s"Unknown arithmetic op: $opName")
    ops += arithOp
    (arithOp.results.head.asInstanceOf[Value[Attribute]], lTy)

  private def emitGetCol(
      sel: SelectExpr,
      tupleArg: Value[Attribute],
      ops: scala.collection.mutable.ArrayBuffer[Operation],
  ): (Value[Attribute], String) =
    val (tableName, colName) = extractColumnRefFromSelect(sel)
    val colType = lookupColumnType(tableName, colName)
    val colRef = mkColRef(tableName, colName)
    val getCol = GetCol(
      tuple = tupleArg.asInstanceOf[Value[TupleType]],
      attr = colRef,
      result = Result(colType),
    )
    ops += getCol
    (getCol.result.asInstanceOf[Value[Attribute]], colType.toString)

  private def emitConstant(
      lit: Literal,
      ops: scala.collection.mutable.ArrayBuffer[Operation],
  ): (Value[Attribute], String) =
    val (value, typ) = lit.ast match
      case d: Expr.DateLit =>
        (d.$value.toString, DateType(StringData("day")).asInstanceOf[Attribute])
      case d: Expr.DoubleLit =>
        (
          d.$value.toString,
          DecimalType(IntData(12), IntData(2))
            .asInstanceOf[Attribute],
        )
      case i: Expr.IntLit =>
        (i.$value.toString, I32.asInstanceOf[Attribute])
      case s: Expr.StringLit =>
        (s.$value, DBStringType().asInstanceOf[Attribute])
      case b: Expr.BooleanLit =>
        (b.$value.toString, I1.asInstanceOf[Attribute])
      case _ =>
        (lit.stringRep, DBStringType().asInstanceOf[Attribute])
    val constOp = DBConstant(
      value = StringData(value),
      result = Result(typ),
    )
    ops += constOp
    (constOp.result.asInstanceOf[Value[Attribute]], typ.toString)

  // ── Aggregation handling ──

  private def containsAggregation(node: QueryIRNode): Boolean =
    node match
      case a: AttrExpr => containsAggregation(a.child)
      case u: UnaryExprOp =>
        u.ast match
          case _: AggregationExpr[?] => true
          case _                     => containsAggregation(u.child)
      case b: BinExprOp =>
        containsAggregation(b.lhs) || containsAggregation(b.rhs)
      case _: Literal        => false
      case _: SelectExpr     => false
      case _: QueryIRVar     => false
      case pc: ProjectClause => pc.children.exists(containsAggregation)
      case _                 => false

  private def emitAggregationFromProjection(
      rel: Value[Attribute],
      projNodes: Seq[QueryIRNode],
      ops: scala.collection.mutable.ArrayBuffer[Operation],
  ): Value[Attribute] =
    emitAggregationOp(rel, Seq(), projNodes, ops)

  private def emitAggregationOp(
      rel: Value[Attribute],
      groupByColRefs: Seq[ColumnRefAttr],
      projNodes: Seq[QueryIRNode],
      ops: scala.collection.mutable.ArrayBuffer[Operation],
  ): Value[Attribute] =
    val aggrInfos = projNodes.flatMap(extractAggregations)

    // Check if any aggregation targets are computed expressions (not simple column refs).
    // If so, emit a relalg.map first to compute those expressions.
    val computedExprs = aggrInfos.collect {
      case (name, Some((func, Right(exprNode))), resultType) =>
        (name, exprNode, resultType)
    }

    val mappedRel = if computedExprs.nonEmpty then
      val mapColDefs = computedExprs.map { (name, _, typ) =>
        mkColDef("map", name, typ)
      }
      val mapOp = MapOp(
        rel = rel.asInstanceOf[Value[TupleStreamType]],
        computedCols = ArrayAttribute(mapColDefs),
        lambda = Region(Seq(
          Block(
            Seq(tupleT),
            (args: Iterable[Value[Attribute]]) =>
              val tupleArg = args.head
              val regionOps = scala.collection.mutable.ArrayBuffer[Operation]()
              val resultVals = computedExprs.map { (_, exprNode, _) =>
                emitPredicate(exprNode, tupleArg, regionOps)._1
              }
              val ret = TuplesReturn(results_ = resultVals)
              regionOps += ret
              regionOps.toSeq,
          )
        )),
        result = Result(ts),
      )
      ops += mapOp
      mapOp.result.asInstanceOf[Value[Attribute]]
    else rel

    // Build column refs for aggregation, using mapped column names for computed exprs
    val resolvedInfos = aggrInfos.map {
      case (name, Some((func, Left(ref))), resultType) =>
        (name, Some((func, ref)), resultType)
      case (name, Some((func, Right(_))), resultType) =>
        (name, Some((func, mkColRef("map", name))), resultType)
      case (name, None, resultType) =>
        (name, None, resultType)
    }

    val computedCols = resolvedInfos.map {
      case (name, _, resultType) =>
        mkColDef("aggr", name, resultType)
    }

    val aggrOp = RelAlgAggregation(
      rel = mappedRel.asInstanceOf[Value[TupleStreamType]],
      groupByCols = ArrayAttribute(groupByColRefs),
      computedCols = ArrayAttribute(computedCols),
      aggr = Region(Seq(
        Block(
          Seq(ts, tupleT),
          (args: Iterable[Value[Attribute]]) =>
            val streamArg = args.head
            val tupleArg = args.tail.head
            val regionOps =
              scala.collection.mutable.ArrayBuffer[Operation]()

            val resultVals = resolvedInfos.map {
              case (name, aggrFunc, resultType) =>
                aggrFunc match
                  case None =>
                    val countOp = CountRows(
                      rel = streamArg.asInstanceOf[Value[TupleStreamType]],
                      result = Result(resultType),
                    )
                    regionOps += countOp
                    countOp.result.asInstanceOf[Value[Attribute]]
                  case Some((func, ref)) =>
                    val aggrFnOp = AggrFn(
                      rel = streamArg.asInstanceOf[Value[TupleStreamType]],
                      fn = func,
                      attr = ref,
                      result = Result(resultType),
                    )
                    regionOps += aggrFnOp
                    aggrFnOp.result.asInstanceOf[Value[Attribute]]
            }

            val ret = TuplesReturn(results_ = resultVals)
            regionOps += ret
            regionOps.toSeq,
        )
      )),
      result = Result(ts),
    )
    ops += aggrOp
    aggrOp.result

  /** Aggregation target: Left = simple column ref, Right = computed expression node */
  private type AggrTargetRef = Either[ColumnRefAttr, QueryIRNode]

  /** Extract aggregation info from projection nodes. */
  private def extractAggregations(
      node: QueryIRNode
  ): Seq[
    (String, Option[(AggrFunc, AggrTargetRef)], Attribute)
  ] =
    node match
      case attr: AttrExpr =>
        val name = attr.projectedName.getOrElse("result")
        extractSingleAggregation(attr.child, name)
      case pc: ProjectClause =>
        pc.children.flatMap(extractAggregations)
      case _ =>
        extractSingleAggregation(node, "result")

  private def extractSingleAggregation(
      node: QueryIRNode,
      name: String,
  ): Seq[
    (String, Option[(AggrFunc, AggrTargetRef)], Attribute)
  ] =
    node match
      case unary: UnaryExprOp =>
        unary.ast match
          case _: AggregationExpr.Sum[?] =>
            val (target, typ) = extractAggrTarget(unary.child)
            Seq((name, Some((AggrFunc.sum, target)), typ))
          case _: AggregationExpr.Avg[?] =>
            val (target, _) = extractAggrTarget(unary.child)
            val avgType = DecimalType(IntData(31), IntData(21)).asInstanceOf[Attribute]
            Seq((name, Some((AggrFunc.avg, target)), avgType))
          case _: AggregationExpr.Min[?] =>
            val (target, typ) = extractAggrTarget(unary.child)
            Seq((name, Some((AggrFunc.min, target)), typ))
          case _: AggregationExpr.Max[?] =>
            val (target, typ) = extractAggrTarget(unary.child)
            Seq((name, Some((AggrFunc.max, target)), typ))
          case _: AggregationExpr.Count[?] =>
            Seq((name, None, I64.asInstanceOf[Attribute]))
          case _ => Seq()
      case lit: Literal =>
        lit.ast match
          case _: AggregationExpr.CountAll =>
            Seq((name, None, I64.asInstanceOf[Attribute]))
          case _ => Seq()
      case _ => Seq()

  /** Returns (Left(colRef) for simple column, Right(exprNode) for computed expression, type) */
  private def extractAggrTarget(
      node: QueryIRNode
  ): (AggrTargetRef, Attribute) =
    node match
      case sel: SelectExpr =>
        val (tableName, colName) = extractColumnRefFromSelect(sel)
        val colType = lookupColumnType(tableName, colName)
        (Left(mkColRef(tableName, colName)), colType)
      case bin: BinExprOp =>
        // Computed expression — infer type from the leftmost column
        val typ = inferExprType(bin)
        (Right(bin), typ)
      case _ =>
        throw Exception(
          s"Cannot extract aggregation target from: ${node.getClass.getSimpleName}"
        )

  /** Compute the result type of an arithmetic op on two types,
    * following SQL/LingoDB decimal arithmetic rules.
    */
  private def computeArithResultType(
      opName: String,
      lhsType: Attribute,
      rhsType: Attribute,
  ): Attribute =
    (opName, lhsType, rhsType) match
      case ("db.mul", DecimalType(IntData(p1), IntData(s1)), DecimalType(IntData(p2), IntData(s2))) =>
        val rawP = p1 + p2
        val rawS = s1 + s2
        if rawP > 38 then
          val adjS = (rawS - (rawP - 38)) max 6
          DecimalType(IntData(38), IntData(adjS))
        else DecimalType(IntData(rawP), IntData(rawS))
      case _ => lhsType // db.add/db.sub keep operand type in LingoDB

  /** Infer the result type of an expression by looking at column refs and arithmetic. */
  private def inferExprType(node: QueryIRNode): Attribute =
    node match
      case sel: SelectExpr =>
        val (tableName, colName) = extractColumnRefFromSelect(sel)
        lookupColumnType(tableName, colName)
      case bin: BinExprOp =>
        val lType = inferExprType(bin.lhs)
        val rType = inferExprType(bin.rhs)
        val opName = bin.ast match
          case _: Expr.Plus[?]  => "db.add"
          case _: Expr.Minus[?] => "db.sub"
          case _: Expr.Times[?] => "db.mul"
          case _                => "db.add"
        computeArithResultType(opName, lType, rType)
      case unary: UnaryExprOp => inferExprType(unary.child)
      case lit: Literal =>
        lit.ast match
          case _: Expr.DoubleLit  => DecimalType(IntData(12), IntData(2))
          case _: Expr.IntLit     => I32
          case _: Expr.DateLit    => DateType(StringData("day"))
          case _: Expr.StringLit  => DBStringType()
          case _: Expr.BooleanLit => I1
          case _                  => DecimalType(IntData(12), IntData(2))
      case _ => DecimalType(IntData(12), IntData(2)) // fallback

  private def emitMapFromProjection(
      rel: Value[Attribute],
      projNodes: Seq[QueryIRNode],
      ops: scala.collection.mutable.ArrayBuffer[Operation],
  ): Value[Attribute] =
    val colRefs = projNodes.flatMap(extractProjectionColumns)
    val projOp = Projection(
      rel = rel.asInstanceOf[Value[TupleStreamType]],
      setSemantic = SetSemantic.all,
      cols = ArrayAttribute(colRefs),
      result = Result(ts),
    )
    ops += projOp
    projOp.result

  private def extractProjectionColumns(
      node: QueryIRNode
  ): Seq[ColumnRefAttr] =
    node match
      case attr: AttrExpr =>
        extractProjectionColumns(attr.child)
      case sel: SelectExpr =>
        val (tableName, colName) = extractColumnRefFromSelect(sel)
        Seq(mkColRef(tableName, colName))
      case pc: ProjectClause =>
        pc.children.flatMap(extractProjectionColumns)
      case _ => Seq()

  // ── Column reference extraction ──

  /** Resolve a RelationOp chain to the underlying table name. */
  private def resolveTableName(op: RelationOp): String =
    op match
      case t: TableLeaf      => t.tableName
      case s: SelectAllQuery => resolveTableName(s.from.head)
      case s: SelectQuery    => resolveTableName(s.from.head)
      case o: OrderedQuery   => resolveTableName(o.query)
      case g: GroupByQuery   => resolveTableName(g.source)
      case n: NaryRelationOp =>
        n.children.head match
          case r: RelationOp => resolveTableName(r)
          case _             => op.alias
      case _                 => op.alias

  private def extractColumnRefFromSelect(sel: SelectExpr): (String, String) =
    sel.from match
      case v: QueryIRVar =>
        v.toSub match
          case g: GroupByQuery =>
            // Check if this column is a group-by key or an aggregation result
            resolveGroupByColumnRef(g, sel.attrName)
          case _ =>
            (resolveTableName(v.toSub), sel.attrName)
      case _ => ("unknown", sel.attrName)

  /** For columns referenced from a GroupByQuery result, find the correct scope.
    * Group-by key columns use their original table scope.
    * Aggregation result columns use "aggr" scope.
    */
  private def resolveGroupByColumnRef(
      g: GroupByQuery,
      colName: String,
  ): (String, String) =
    // Check if it's a group-by key column
    val groupByColumns = collectNamedColumns(g.groupBy)
    groupByColumns.find(_._1 == colName) match
      case Some((_, tableName, origColName)) =>
        (tableName, origColName)
      case None =>
        // Check if it's from the projection (aggregation result)
        g.source match
          case sq: SelectQuery =>
            val projColumns = collectNamedColumns(sq.project)
            projColumns.find(_._1 == colName) match
              case Some((_, tableName, origColName)) =>
                (tableName, origColName)
              case None =>
                // It's likely an aggregation computed column
                ("aggr", colName)
          case _ =>
            ("aggr", colName)

  /** Collect named columns from projection/groupBy nodes.
    * Returns (projectedName, tableName, originalColName).
    */
  private def collectNamedColumns(
      node: QueryIRNode
  ): Seq[(String, String, String)] =
    node match
      case pc: ProjectClause =>
        pc.children.flatMap(collectNamedColumns)
      case attr: AttrExpr =>
        attr.projectedName match
          case Some(name) =>
            attr.child match
              case sel: SelectExpr =>
                val (tableName, colName) = sel.from match
                  case v: QueryIRVar => (resolveTableName(v.toSub), sel.attrName)
                  case _             => ("unknown", sel.attrName)
                Seq((name, tableName, colName))
              case _ => Seq()
          case None => Seq()
      case _ => Seq()

  private def extractColumnRef(node: QueryIRNode): (String, String) =
    node match
      case sel: SelectExpr => extractColumnRefFromSelect(sel)
      case attr: AttrExpr  => extractColumnRef(attr.child)
      case _ =>
        throw Exception(
          s"Cannot extract column ref from: ${node.getClass.getSimpleName}"
        )

  private def extractGroupByColumns(
      node: QueryIRNode
  ): Seq[ColumnRefAttr] =
    node match
      case pc: ProjectClause =>
        pc.children.flatMap(extractGroupByColumns)
      case attr: AttrExpr =>
        extractGroupByColumns(attr.child)
      case sel: SelectExpr =>
        val (tableName, colName) = extractColumnRefFromSelect(sel)
        Seq(mkColRef(tableName, colName))
      case bin: BinExprOp =>
        extractGroupByColumns(bin.lhs) ++ extractGroupByColumns(bin.rhs)
      case _ => Seq()

  private def lookupColumnType(
      tableName: String,
      colName: String,
  ): Attribute =
    schemas
      .get(tableName)
      .flatMap(_.columns.get(colName))
      .getOrElse(
        throw Exception(s"Unknown column: $tableName.$colName")
      )

  private def flattenAnd(node: QueryIRNode): Seq[QueryIRNode] =
    node match
      case bin: BinExprOp if bin.ast.isInstanceOf[Expr.And] =>
        flattenAnd(bin.lhs) ++ flattenAnd(bin.rhs)
      case other => Seq(other)

  /** Deeply flatten all AND expressions and WhereClause wrappers into atomic predicates. */
  private def flattenAllAnds(node: QueryIRNode): Seq[QueryIRNode] =
    node match
      case wc: WhereClause => wc.children.flatMap(flattenAllAnds)
      case bin: BinExprOp =>
        bin.ast match
          case _: Expr.And => flattenAllAnds(bin.lhs) ++ flattenAllAnds(bin.rhs)
          case _           => Seq(node)
      case other => Seq(other)

object RelAlgGenerator:
  /** Column mapping for the executable wrapper's materialize + subop.local_table type.
    * @param scope     Column ref scope (e.g. "lineitem", "aggr")
    * @param colName   Column ref name (e.g. "l_returnflag", "sum_0")
    * @param outputName Display name in the result table (e.g. "l_returnflag", "sum_qty")
    * @param colType   MLIR type of the column
    */
  case class OutputColumn(scope: String, colName: String, outputName: String, colType: Attribute)

