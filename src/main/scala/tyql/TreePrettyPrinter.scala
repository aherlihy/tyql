package tyql

import language.experimental.namedTuples
import NamedTuple.NamedTuple

/**
 * Convenience printer for AST + IR trees, for debugging.
 */
object TreePrettyPrinter {
  import Query.*
  import Expr.*
  import AggregationExpr.*

  private def indent(level: Int): String = scala.collection.StringOps("  ") * level // without explicit StringOps, treated as Expr[String] * Int
  private def indentWithKey(level: Int, key: String, value: String): String = s"${indent(level)}$key=${value.stripLeading()}"
  private def indentListWithKey(level: Int, key: String, values: Seq[String]): String =
    if (values.isEmpty)
      s"${indent(level)}$key=[]"
    else
      s"${indent(level)}$key=${values.mkString("[\n", ",\n", s"\n${indent(level)}]")}"

  extension (fun: Fun[?, ?, ?]) {
    def prettyPrint(depth: Int): String = fun match
      case Fun(param, body: Expr[?, ?]) =>
        s"${indent(depth)}FunE(${param.stringRef()} =>\n${body.prettyPrint(depth + 1)}\n${indent(depth)})"
      case Fun(param, body: DatabaseAST[?]) =>
        s"${indent(depth)}FunQ(${param.stringRef()} =>\n${body.prettyPrint(depth + 1)}\n${indent(depth)})"
      case _ => throw new Exception(s"Unimplemented pretty print FUN $fun")
  }

  extension (expr: Expr[?, ?]) {
    def prettyPrint(depth: Int): String = expr match {
      case Select(x, name) => s"${indent(depth)}Select(${x.prettyPrint(0)}.$name)"
      case Ref(_) => s"${indent(depth)}${expr.asInstanceOf[Ref[?, ?]].stringRef()}"
      case Eq(x, y) => s"${indent(depth)}Eq(\n${x.prettyPrint(depth + 1)},\n${y.prettyPrint(depth + 1)}\n${indent(depth)})"
      case Ne(x, y) => s"${indent(depth)}Ne(\n${x.prettyPrint(depth + 1)},\n${y.prettyPrint(depth + 1)}\n${indent(depth)})"
      case Gt(x, y) => s"${indent(depth)}Gt(\n${x.prettyPrint(depth + 1)},\n${y.prettyPrint(depth + 1)}\n${indent(depth)})"
      case Lt(x, y) => s"${indent(depth)}Lt(\n${x.prettyPrint(depth + 1)},\n${y.prettyPrint(depth + 1)}\n${indent(depth)})"
      case Lte(x, y) => s"${indent(depth)}Lte(\n${x.prettyPrint(depth + 1)},\n${y.prettyPrint(depth + 1)}\n${indent(depth)})"
      case Gte(x, y) => s"${indent(depth)}Gte(\n${x.prettyPrint(depth + 1)},\n${y.prettyPrint(depth + 1)}\n${indent(depth)})"
      case ListExpr(elements) =>
        s"${indent(depth)}ListExpr(\n${elements.map(_.prettyPrint(depth + 1)).mkString("\n")}\n${indent(depth)}"
      case ListPrepend(x, list) =>
        s"${indent(depth)}ListPrepend(\n${x.prettyPrint(depth + 1)},\n${list.prettyPrint(depth + 1)}\n${indent(depth)})"
      case ListAppend(list, x) =>
        s"${indent(depth)}ListAppend(\n${x.prettyPrint(depth + 1)},\n${list.prettyPrint(depth + 1)}\n${indent(depth)})"
      case ListContains(list, x) =>
        s"${indent(depth)}ListContains(\n${x.prettyPrint(depth + 1)},\n${list.prettyPrint(depth + 1)}\n${indent(depth)})"
      case ListLength(list) =>
        s"${indent(depth)}ListLength(\n${list.prettyPrint(depth + 1)}\n${indent(depth)})"
      case NonEmpty(list) =>
        s"${indent(depth)}NonEmpty(\n${list.prettyPrint(depth + 1)}\n${indent(depth)})"
      case IsEmpty(list) =>
        s"${indent(depth)}IsEmpty(\n${list.prettyPrint(depth + 1)}\n${indent(depth)})"
      case And(x, y) => s"${indent(depth)}And(\n${x.prettyPrint(depth + 1)},\n${y.prettyPrint(depth + 1)}\n${indent(depth)})"
      case Or(x, y) => s"${indent(depth)}Or(\n${x.prettyPrint(depth + 1)},\n${y.prettyPrint(depth + 1)}\n${indent(depth)})"
      case Not(x) => s"${indent(depth)}Not(${x.prettyPrint(depth + 1)})"
      case Plus(x, y) => s"${indent(depth)}Plus(\n${x.prettyPrint(depth + 1)},\n${y.prettyPrint(depth + 1)}\n${indent(depth)})"
      case Times(x, y) => s"${indent(depth)}Times(\n${x.prettyPrint(depth + 1)},\n${y.prettyPrint(depth + 1)}\n${indent(depth)})"
      case Upper(x) => s"${indent(depth)}Upper(${x.prettyPrint(depth + 1)})"
      case Lower(x) => s"${indent(depth)}Lower(${x.prettyPrint(depth + 1)})"
      case Concat(x, y) =>
        s"${indent(depth)}Concat(\n${x.prettyPrint(depth + 1)},\n${y.prettyPrint(depth + 1)}\n${indent(depth)})"
      case IntLit(value) => s"${indent(depth)}IntLit($value)"
      case StringLit(value) => s"${indent(depth)}StringLit($value)"
      case DoubleLit(value) => s"${indent(depth)}DoubleLit($value)"
      case BooleanLit(value) => s"${indent(depth)}BooleanLit($value)"
      case Project(inner) =>
        val a = NamedTuple.toTuple(inner.asInstanceOf[NamedTuple[Tuple, Tuple]]) // TODO: bug? See https://github.com/scala/scala3/issues/21157
        val namedTupleNames = expr.tag match
          case ResultTag.NamedTupleTag(names, types) => names.lift
          case _ => Seq()
        val children = a.toList.zipWithIndex
          .map((expr, idx) =>
            val e = expr.asInstanceOf[Expr[?, ?]]
            val namedStr = namedTupleNames(idx).fold("")(n => s"$n")
            indentWithKey(depth + 1, namedStr, e.prettyPrint(depth + 1))
          )
        s"${indent(depth)}Project(\n${children.mkString(",\n")}\n${indent(depth)})"
      case a: AggregationExpr[?] => a.prettyPrint(depth)
      case a: Aggregation[?, ?] => a.prettyPrint(depth)
      case NullLit() => s"${indent(depth)}Null"
      case IsNull(x) => s"${indent(depth)}IsNull(\n${x.prettyPrint(depth + 1)}\n${indent(depth)})"
      case Coalesce(x, y, xs) =>
        val children = (x +: y +: xs).map(_.prettyPrint(depth + 1))
        s"${indent(depth)}Coalesce(\n${children.mkString(",\n")}\n${indent(depth)})"
      case NullIf(x, y) =>
        s"${indent(depth)}NullIf(\n${x.prettyPrint(depth + 1)},\n${y.prettyPrint(depth + 1)}\n${indent(depth)})"
      case Xor(x, y) => s"${indent(depth)}Xor(\n${x.prettyPrint(depth + 1)},\n${y.prettyPrint(depth + 1)}\n${indent(depth)})"
      case Modulo(x, y) => s"${indent(depth)}Modulo(\n${x.prettyPrint(depth + 1)},\n${y.prettyPrint(depth + 1)}\n${indent(depth)})"
      case Cast(x, resultType) => s"${indent(depth)}Cast(\n${x.prettyPrint(depth + 1)},\n${indent(depth + 1)}$resultType\n${indent(depth)})"
      case SearchedCase(cases, elseCase) =>
        val casesStr = cases.map { case (cond, value) =>
          s"${indent(depth + 1)}WHEN ${cond.prettyPrint(0)} THEN ${value.prettyPrint(0)}"
        }.mkString("\n")
        val elseStr = elseCase.map(e => s"\n${indent(depth + 1)}ELSE ${e.prettyPrint(0)}").getOrElse("")
        s"${indent(depth)}SearchedCase(\n$casesStr$elseStr\n${indent(depth)})"
      case SimpleCase(expr, cases, elseCase) =>
        val exprStr = expr.prettyPrint(depth + 1)
        val casesStr = cases.map { case (cond, value) =>
          s"${indent(depth + 1)}WHEN ${cond.prettyPrint(0)} THEN ${value.prettyPrint(0)}"
        }.mkString("\n")
        val elseStr = elseCase.map(e => s"\n${indent(depth + 1)}ELSE ${e.prettyPrint(0)}").getOrElse("")
        s"${indent(depth)}SimpleCase(\n$exprStr\n$casesStr$elseStr\n${indent(depth)})"
      case OptionMap(x, f) => s"${indent(depth)}OptionMap(\n${x.prettyPrint(depth + 1)},\n<<<<<<UNCLEAR HOW TO PRINT A FUNCTION>>>>>>\n${indent(depth)})"
      case StringCharLength(x) => s"${indent(depth)}StringCharLength(${x.prettyPrint(0)})"
      case StringByteLength(x) => s"${indent(depth)}StringByteLength(${x.prettyPrint(0)})"
      case LTrim(x) => s"${indent(depth)}LTrim(${x.prettyPrint(0)})"
      case RTrim(x) => s"${indent(depth)}RTrim(${x.prettyPrint(0)})"
      case Trim(x) => s"${indent(depth)}Trim(${x.prettyPrint(0)})"
      case StrReplace(s, from, to) => s"${indent(depth)}StrReplace(\n${s.prettyPrint(depth + 1)},\n${from.prettyPrint(depth + 1)},\n${to.prettyPrint(depth + 1)}\n${indent(depth)})"
      case Substring(s, from, len) =>
        val lenStr = len.map(l => s",\n${l.prettyPrint(depth + 1)}").getOrElse("")
        s"${indent(depth)}Substring(\n${s.prettyPrint(depth + 1)},\n${from.prettyPrint(depth + 1)}$lenStr\n${indent(depth)})"
      case StrLike(s, pattern) => s"${indent(depth)}StrLike(\n${s.prettyPrint(depth + 1)},\n${pattern.prettyPrint(depth + 1)}\n${indent(depth)})"
      case StrConcat(x, xs) => s"${indent(depth)}StrConcat(\n${x.prettyPrint(depth + 1)},\n${xs.map(_.prettyPrint(depth + 1)).mkString(",\n")}\n${indent(depth)})"
      case StrConcatUniform(x, xs) => s"${indent(depth)}StrConcatUniform(\n${x.prettyPrint(depth + 1)},\n${xs.map(_.prettyPrint(depth + 1)).mkString(",\n")}\n${indent(depth)})"
      case StrConcatSeparator(sep, x, xs) => s"${indent(depth)}StrConcatSeparator(\n${sep.prettyPrint(depth + 1)},\n${x.prettyPrint(depth + 1)},\n${xs.map(_.prettyPrint(depth + 1)).mkString(",\n")}\n${indent(depth)})"
      case StrReverse(x) => s"${indent(depth)}StrReverse(${x.prettyPrint(0)})"
      case StrRepeat(s, n) => s"${indent(depth)}StrRepeat(\n${s.prettyPrint(depth + 1)},\n${n.prettyPrint(depth + 1)}\n${indent(depth)})"
      case StrLPad(s, len, pad) => s"${indent(depth)}StrLPad(\n${s.prettyPrint(depth + 1)},\n${len.prettyPrint(depth + 1)},\n${pad.prettyPrint(depth + 1)}\n${indent(depth)})"
      case StrRPad(s, len, pad) => s"${indent(depth)}StrRPad(\n${s.prettyPrint(depth + 1)},\n${len.prettyPrint(depth + 1)},\n${pad.prettyPrint(depth + 1)}\n${indent(depth)})"
      case StrPositionIn(substr, string) => s"${indent(depth)}StrPositionIn(\n${substr.prettyPrint(depth + 1)},\n${string.prettyPrint(depth + 1)}\n${indent(depth)})"
      case NullSafeEq(x, y) => s"${indent(depth)}NullSafeEq(\n${x.prettyPrint(depth + 1)},\n${y.prettyPrint(depth + 1)}\n${indent(depth)})"
      case NullSafeNe(x, y) => s"${indent(depth)}NullSafeNe(\n${x.prettyPrint(depth + 1)},\n${y.prettyPrint(depth + 1)}\n${indent(depth)})"
      case Round(x) => s"${indent(depth)}Round(${x.prettyPrint(0)})"
      case RoundWithPrecision(x, precision) => s"${indent(depth)}RoundWithPrecision(\n${x.prettyPrint(depth + 1)},\n${precision.prettyPrint(depth + 1)}\n${indent(depth)})"
      case Ceil(x) => s"${indent(depth)}Ceil(${x.prettyPrint(0)})"
      case Floor(x) => s"${indent(depth)}Floor(${x.prettyPrint(0)})"
      case Power(x, y) => s"${indent(depth)}Power(\n${x.prettyPrint(depth + 1)},\n${y.prettyPrint(depth + 1)}\n${indent(depth)})"
      case Sqrt(x) => s"${indent(depth)}Sqrt(${x.prettyPrint(0)})"
      case Abs(x) => s"${indent(depth)}Abs(${x.prettyPrint(0)})"
      case Sign(x) => s"${indent(depth)}Sign(${x.prettyPrint(0)})"
      case LogNatural(x) => s"${indent(depth)}LogNatural(${x.prettyPrint(0)})"
      case Log(base, x) => s"${indent(depth)}Log(\n${base.prettyPrint(depth + 1)},\n${x.prettyPrint(depth + 1)}\n${indent(depth)})"
      case Exp(x) => s"${indent(depth)}Exp(${x.prettyPrint(0)})"
      case Sin(x) => s"${indent(depth)}Sin(${x.prettyPrint(0)})"
      case Cos(x) => s"${indent(depth)}Cos(${x.prettyPrint(0)})"
      case Tan(x) => s"${indent(depth)}Tan(${x.prettyPrint(0)})"
      case Asin(x) => s"${indent(depth)}Asin(${x.prettyPrint(0)})"
      case Acos(x) => s"${indent(depth)}Acos(${x.prettyPrint(0)})"
      case Atan(x) => s"${indent(depth)}Atan(${x.prettyPrint(0)})"
      case RandomUUID() => s"${indent(depth)}RandomUUID()"
      case RandomFloat() => s"${indent(depth)}RandomFloat()"
      case RandomInt(x, y) => s"${indent(depth)}RandomInt(\n${x.prettyPrint(depth + 1)},\n${y.prettyPrint(depth + 1)}\n${indent(depth)})"
      case _ => throw new Exception(s"Unimplemented pretty print EXPR $expr")
    }
  }
  extension(agg: Aggregation[?, ?]) {
    def prettyPrint(depth: Int): String = agg match {
      case Aggregation.AggFlatMap(from, query) =>
        s"${indent(depth)}AggFlatMap(\n${from.prettyPrint(depth + 1)},\n${query.prettyPrint(depth + 1)}\n${indent(depth)})"
      case Aggregation.AggFilter(from, query) =>
        s"${indent(depth)}AggFilter(\n${from.prettyPrint(depth + 1)},\n${query.prettyPrint(depth + 1)}\n${indent(depth)})"
    }
  }
  extension(agg: AggregationExpr[?]) {
    def prettyPrint(depth: Int): String = agg match {
      case Min(x) => s"${indent(depth)}Min(${x.prettyPrint(depth + 1).stripLeading()})"
      case Max(x) => s"${indent(depth)}Max(${x.prettyPrint(depth + 1).stripLeading()})"
      case Sum(x) => s"${indent(depth)}Sum(${x.prettyPrint(depth + 1).stripLeading()})"
      case Avg(x) => s"${indent(depth)}Avg(${x.prettyPrint(depth + 1).stripLeading()})"
      case Count(x) => s"${indent(depth)}Count(${x.prettyPrint(depth + 1).stripLeading()})"
      case AggProject(inner) =>
        val a = NamedTuple.toTuple(inner.asInstanceOf[NamedTuple[Tuple, Tuple]]) // TODO: bug? See https://github.com/scala/scala3/issues/21157
        val namedTupleNames = agg.tag match
          case ResultTag.NamedTupleTag(names, types) => names.lift
          case _ => Seq()
        val children = a.toList.zipWithIndex
          .map((expr, idx) =>
            val e = expr.asInstanceOf[Expr[?, ?]]
            val namedStr = namedTupleNames(idx).fold("")(n => s"$n=")
            s"${indent(depth+1)}$namedStr${e.prettyPrint(0)}"
          )
        s"${indent(depth)}AggProject(\n${children.mkString(",\n")}\n${indent(depth)})"
      case _ => throw new Exception(s"Unimplemented pretty print AGG $agg")
    }
  }

  extension (ast: DatabaseAST[?]) {
    def prettyPrint(depth: Int): String = ast match {
      case Table(name) =>
        s"${indent(depth)}Table($name)"
      case Filter(from, pred) =>
        s"${indent(depth)}Filter(\n${from.prettyPrint(depth + 1)},\n${pred.prettyPrint(depth + 1)}\n${indent(depth)})"
      case Map(from, query) =>
        s"${indent(depth)}Map(\n${from.prettyPrint(depth + 1)},\n${query.prettyPrint(depth + 1)}\n${indent(depth)})"
      case FlatMap(from, query) =>
        s"${indent(depth)}FlatMap(\n${from.prettyPrint(depth + 1)},\n${query.prettyPrint(depth + 1)}\n${indent(depth)})"
      case Sort(from, body, ord) =>
        s"${indent(depth)}Sort(ord=$ord\n${from.prettyPrint(depth + 1)},\n${body.prettyPrint(depth + 1)}\n${indent(depth)})"
      case Limit(from, limit) =>
        s"${indent(depth)}Limit(limit=$limit\n${from.prettyPrint(depth + 1)}\n${indent(depth)})"
      case Offset(from, offset) =>
        s"${indent(depth)}Offset(offset=$offset\n${from.prettyPrint(depth + 1)}\n${indent(depth)})"
      case Distinct(from) =>
        s"${indent(depth)}Distinct(\n${from.prettyPrint(depth + 1)}\n${indent(depth)})"
      case Union(thisQuery, other) =>
        s"${indent(depth)}Union(\n${thisQuery.prettyPrint(depth + 1)},\n${other.prettyPrint(depth + 1)}\n${indent(depth)})"
      case Intersect(thisQuery, other) =>
        s"${indent(depth)}Intersect(\n${thisQuery.prettyPrint(depth + 1)},\n${other.prettyPrint(depth + 1)}\n${indent(depth)})"
      case Except(thisQuery, other) =>
        s"${indent(depth)}Except(\n${thisQuery.prettyPrint(depth + 1)},\n${other.prettyPrint(depth + 1)}\n${indent(depth)})"
      case UnionAll(thisQuery, other) =>
        s"${indent(depth)}UnionAll(\n${thisQuery.prettyPrint(depth + 1)},\n${other.prettyPrint(depth + 1)}\n${indent(depth)})"
      case IntersectAll(thisQuery, other) =>
        s"${indent(depth)}IntersectAll(\n${thisQuery.prettyPrint(depth + 1)},\n${other.prettyPrint(depth + 1)}\n${indent(depth)})"
      case ExceptAll(thisQuery, other) =>
        s"${indent(depth)}ExceptAll(\n${thisQuery.prettyPrint(depth + 1)},\n${other.prettyPrint(depth + 1)}\n${indent(depth)})"
      case MultiRecursive(refs, querys, finalQ) =>
        val refStr = refs.toList.map(r => r.toQuery.prettyPrint(depth+1))
        val qryStr = querys.toList.map(q => q.asInstanceOf[Query[?, ?]].prettyPrint(depth + 2))
        val str = refStr.zip(qryStr).map((r, q) => s"\n$r :=\n$q").mkString(",\n")
        val finalQStr = finalQ.prettyPrint(depth + 1)
        s"${indent(depth)}MultiRecursive($str\n${indent(depth)}\n${indentWithKey(depth+1, "FINAL->", finalQStr)}\n${indent(depth)})"
      case QueryRef() => s"${indent(depth)}QueryRef(${ast.asInstanceOf[QueryRef[?, ?]].stringRef()})"
      case a: Aggregation[?, ?] => a.prettyPrint(depth)
      case GroupBy(source, grouping, select, having) =>
        s"${indent(depth)}GroupBy(\n${source.prettyPrint(depth + 1)},\n${grouping.prettyPrint(depth + 1)},\n${select.prettyPrint(depth + 1)},\n${having.map(_.prettyPrint(depth + 1)).getOrElse(s"${indent(depth+1)}-")}\n${indent(depth)})"
      case NewGroupBy(source, grouping, refs, tag, having) =>
        s"${indent(depth)}NewGroupBy(\n${source.prettyPrint(depth + 1)},\n${grouping.prettyPrint(depth + 1)},\ntag=${tag},\n${having.map(_.prettyPrint(depth + 1)).getOrElse(s"${indent(depth+1)}-")}\n${indent(depth)})"
      case _ => throw new Exception(s"Unimplemented pretty print AST $ast")
    }
  }

  extension (relationOp: RelationOp) {
    def prettyPrintIR(depth: Int, printAST: Boolean): String = relationOp match {
      case tableLeaf: TableLeaf =>
        val astPrint = if (printAST) s"\n${indentWithKey(depth + 1, "AST", tableLeaf.ast.prettyPrint(depth + 1))}" else ""
        s"${indent(depth)}TableLeaf{${relationOp.alias}{${relationOp.flags.mkString(",")}}(${tableLeaf.tableName}$astPrint)"
      case selectQuery: SelectQuery =>
        val projectPrint =  indentWithKey(depth + 1, "project", selectQuery.project.prettyPrintIR(depth + 1, printAST))
        val fromPrint =     indentListWithKey(depth + 1, "from", selectQuery.from.map(_.prettyPrintIR(depth + 2, printAST)))
        val wherePrint =    indentListWithKey(depth + 1, "where", selectQuery.where.map(_.prettyPrintIR(depth + 2, printAST)))
        val astPrint = if (printAST) s"\n${indentWithKey(depth + 1, "AST", selectQuery.ast.prettyPrint(depth + 1))}" else ""
        s"${indent(depth)}SelectQuery{${relationOp.alias}}{${relationOp.flags.mkString(",")}}(\n$projectPrint,\n$fromPrint,\n$wherePrint$astPrint\n${indent(depth)})"
      case selectAllQuery: SelectAllQuery =>
        val fromPrint =     indentListWithKey(depth + 1, "from", selectAllQuery.from.map(_.prettyPrintIR(depth + 2, printAST)))
        val wherePrint =    indentListWithKey(depth + 1, "where", selectAllQuery.where.map(_.prettyPrintIR(depth + 2, printAST)))
        val astPrint = if (printAST) s"\n${indentWithKey(depth + 1, "AST", selectAllQuery.ast.prettyPrint(depth + 1))}" else ""
        s"${indent(depth)}SelectAllQuery{${relationOp.alias}}{${relationOp.flags.mkString(",")}}(\n$fromPrint,\n$wherePrint$astPrint\n${indent(depth)})"
      case orderedQuery: OrderedQuery =>
        val queryPrint = orderedQuery.query.prettyPrintIR(depth + 1, printAST)
        val sortFnPrint = indentListWithKey(depth+1, "sort", orderedQuery.sortFn.map { case (node, ord) =>
          s"${indent(depth + 2)}${ord.toString}::${node.prettyPrintIR(depth + 2, printAST).stripLeading()}"
        })
        val astPrint = if (printAST) s"\n${indentWithKey(depth + 1, "AST", orderedQuery.ast.prettyPrint(depth + 1))}" else ""
        s"${indent(depth)}OrderedQuery{${relationOp.alias}}{${relationOp.flags.mkString(",")}}(\n$queryPrint,\n$sortFnPrint$astPrint\n${indent(depth)})"
      case naryRelationOp: NaryRelationOp =>
        val childrenPrint = naryRelationOp.children.map(_.prettyPrintIR(depth + 1, printAST))
        val astPrint = if (printAST) s"\n${indentWithKey(depth + 1, "AST", naryRelationOp.ast.prettyPrint(depth + 1))}" else ""
        s"${indent(depth)}N-aryRelationOp{${relationOp.alias}}{${relationOp.flags.mkString(",")}}(\n${indent(depth + 1)}op = '${naryRelationOp.op}'\n${childrenPrint.mkString(",\n")}$astPrint\n${indent(depth)})"
      case MultiRecursiveRelationOp(alias, query, finalQ, carriedSymbols, ast) =>
        val qryStr = query.map(q => q.prettyPrintIR(depth + 1, false))
        val str = alias.zip(qryStr).map((r, q) => s"\n$r => $q").mkString(",\n")
        s"${indent(depth)}MultiRecursive($str\n${indent(depth)})"
      case recursiveIRVar: RecursiveIRVar =>
        s"${indent(depth)}RecursiveVar{${recursiveIRVar.alias}}->${recursiveIRVar.pointsToAlias}"
      case GroupByQuery(source, groupBy, having, overrideAlias, ast) =>
        val srcStr = source.prettyPrintIR(depth + 1, false)
        val groupByStr = groupBy.prettyPrintIR(depth + 1, false)
        val havingStr = having.map(_.prettyPrintIR(depth + 1, false)).getOrElse("[]")
        s"${indent(depth)}GroupBy(\n$srcStr,\n$groupByStr,\n$havingStr\n${indent(depth)})"

      case _ => throw new Exception(s"Unimplemented pretty print RelationOp $relationOp")
    }
  }

  extension (node: QueryIRNode) {
    def prettyPrintIR(depth: Int, printAST: Boolean): String = node match {
      case unaryOp: UnaryExprOp =>
        val childPrint = unaryOp.child.prettyPrintIR(depth + 1, printAST)
        val astPrint = if (printAST) s"\n${indentWithKey(depth + 1, "AST", unaryOp.ast.prettyPrint(depth + 1))}" else ""
        s"${indent(depth)}UnaryExprOp(\n${indent(depth + 1)}op='${unaryOp.pre + "..." + unaryOp.post}',\n$childPrint$astPrint\n${indent(depth)})"
      case binOp: BinExprOp =>
        val lhsPrint = binOp.lhs.prettyPrintIR(depth + 1, printAST)
        val rhsPrint = binOp.rhs.prettyPrintIR(depth + 1, printAST)
        val opPrint = s"${binOp.pre}'..1'${binOp.mid}'..2'${binOp.post}"
        val astPrint = if (printAST) s"\n${indentWithKey(depth + 1, "AST", binOp.ast.prettyPrint(depth + 1))}" else ""
        s"${indent(depth)}BinExprOp(\n${indent(depth + 1)}op='$opPrint',\n$lhsPrint,\n$rhsPrint$astPrint\n${indent(depth)})"
      case listType: ListTypeExpr =>
        val astPrint = if (printAST) s"\n${indentWithKey(depth + 1, "AST", listType.ast.prettyPrint(depth + 1))}" else ""
        s"${indent(depth)}ListTypeExpr(\n${listType.elements.map(_.prettyPrintIR(depth + 1, printAST)).mkString("\n,")}$astPrint\n${indent(depth)})"
      case where: WhereClause =>
        val childrenPrint = where.children.map(_.prettyPrintIR(depth + 2, printAST)).mkString("[\n", ",\n", s"\n${indent(depth+1)}]")
        val astPrint = if (printAST) s"\n${indentWithKey(depth + 1, "AST", where.ast.prettyPrint(depth + 1))}" else ""
        s"${indent(depth)}WhereClause(\n${indent(depth + 1)}$childrenPrint$astPrint\n${indent(depth)})"
      case proj: ProjectClause =>
        val childrenPrint = proj.children.map(_.prettyPrintIR(depth + 2, printAST)).mkString("[\n", ",\n", s"\n${indent(depth+1)}]")
        val astPrint = if (printAST) s"\n${indentWithKey(depth + 1, "AST", proj.ast.prettyPrint(depth + 1))}" else ""
        s"${indent(depth)}ProjectClause(\n${indent(depth + 1)}$childrenPrint$astPrint\n${indent(depth)})"
      case attrExpr: AttrExpr =>
        val childPrint = attrExpr.child.prettyPrintIR(depth + 1, printAST).stripLeading()
        val astPrint = if (printAST) s"\n${indentWithKey(depth + 1, "AST", attrExpr.ast.prettyPrint(depth + 1))}" else ""
        s"${indent(depth)}AttrExpr(\n${indentWithKey(depth + 1, "projectedName", attrExpr.projectedName.getOrElse("None"))},\n${indent(depth + 1)}$childPrint$astPrint\n${indent(depth)})"
      case selectExpr: SelectExpr =>
        val fromPrint = selectExpr.from.prettyPrintIR(depth + 1, printAST)
        val astPrint = if (printAST) s"\n${indentWithKey(depth + 1, "AST", selectExpr.ast.prettyPrint(depth + 1))}" else ""
        s"${indent(depth)}SelectExpr(\n${indent(depth + 1)}attrName='${selectExpr.attrName}',\n$fromPrint$astPrint\n${indent(depth)})"
      case irVar: QueryIRVar =>
        val astPrint = if (printAST) s", ${irVar.ast.prettyPrint(0)}" else ""
        s"${indent(depth)}QueryIRVar(${irVar.name} -> ${irVar.toSub.alias}$astPrint)"
      case literal: Literal =>
        val astPrint = if (printAST) s", ${literal.ast.prettyPrint(0)}" else ""
        s"${indent(depth)}Literal(${literal.stringRep}$astPrint)"
      case empty: EmptyLeaf =>
        s"${indent(depth)}EmptyLeaf"
      case functionCall: FunctionCallOp =>
        val childrenPrint = functionCall.children.map(_.prettyPrintIR(depth + 1, printAST)).mkString(",\n")
        val astPrint = if (printAST) s"\n${indentWithKey(depth + 1, "AST", functionCall.ast.prettyPrint(depth + 1))}" else ""
        s"${indent(depth)}FunctionCall(\n${indent(depth + 1)}name='${functionCall.name}',\n${indent(depth + 1)}args=[\n$childrenPrint\n${indent(depth + 1)}]$astPrint\n${indent(depth)})"
      case searchedCase: SearchedCaseOp =>
        val whenPrint = searchedCase.whenClauses.map { case (cond, res) =>
          s"${indent(depth + 1)}WHEN:\n${cond.prettyPrintIR(depth + 2, printAST)}\n${indent(depth + 1)}THEN:\n${res.prettyPrintIR(depth + 2, printAST)}"
        }.mkString("\n")
        val elsePrint = searchedCase.elseClause.map(e => s"\n${indent(depth + 1)}ELSE:\n${e.prettyPrintIR(depth + 2, printAST)}").getOrElse("")
        val astPrint = if (printAST) s"\n${indentWithKey(depth + 1, "AST", searchedCase.ast.prettyPrint(depth + 1))}" else ""
        s"${indent(depth)}SearchedCase(\n$whenPrint$elsePrint$astPrint\n${indent(depth)})"
      case simpleCase: SimpleCaseOp =>
        val exprPrint = simpleCase.expr.prettyPrintIR(depth + 2, printAST)
        val whenPrint = simpleCase.whenClauses.map { case (cond, res) =>
          s"${indent(depth + 1)}WHEN:\n${cond.prettyPrintIR(depth + 2, printAST)}\n${indent(depth + 1)}THEN:\n${res.prettyPrintIR(depth + 2, printAST)}"
        }.mkString("\n")
        val elsePrint = simpleCase.elseClause.map(e => s"\n${indent(depth + 1)}ELSE:\n${e.prettyPrintIR(depth + 2, printAST)}").getOrElse("")
        val astPrint = if (printAST) s"\n${indentWithKey(depth + 1, "AST", simpleCase.ast.prettyPrint(depth + 1))}" else ""
        s"${indent(depth)}SimpleCase(\n${indent(depth + 1)}expr:\n$exprPrint\n$whenPrint$elsePrint$astPrint\n${indent(depth)})"
      case rawSQL: RawSQLInsertOp =>
        val replacementsPrint = rawSQL.replacements.map { case (name, node) =>
          s"${indent(depth + 2)}$name -> ${node.prettyPrintIR(0, printAST)}"
        }.mkString("\n")
        val astPrint = if (printAST) s"\n${indentWithKey(depth + 1, "AST", rawSQL.ast.prettyPrint(depth + 1))}" else ""
        s"${indent(depth)}RawSQL(\n${indent(depth + 1)}snippet=${rawSQL.snippet},\n${indent(depth + 1)}replacements=[\n$replacementsPrint\n${indent(depth + 1)}]$astPrint\n${indent(depth)})"

      case relationOp: RelationOp => relationOp.prettyPrintIR(depth, printAST)

      case _ => throw new Exception(s"Unimplemented pretty print QueryIRNode $node")
    }
  }
}
