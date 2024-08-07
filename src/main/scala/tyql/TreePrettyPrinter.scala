package tyql

import language.experimental.namedTuples
import NamedTuple.NamedTuple

/**
 * Convenience printer for AST + IR trees, for debugging.
 */
object TreePrettyPrinter {
  import Query.*
  import Expr.*
  import Aggregation.*

  private def indent(level: Int): String = "  " * level
  private def indentWithKey(level: Int, key: String, value: String): String = s"${indent(level)}$key=${value.stripLeading()}"
  private def indentListWithKey(level: Int, key: String, values: Seq[String]): String =
    if (values.isEmpty)
      s"${indent(level)}$key=[]"
//    else if (values.size == 1)
//      s"${indent(level)}$key=[ ${values.head.stripLeading()} ]"
    else
      s"${indent(level)}$key=${values.mkString("[\n", ",\n", s"\n${indent(level)}]")}"


  extension (fun: Fun[?, ?]) {
    def prettyPrint(depth: Int): String = fun match
      case Fun(param, body: Expr[?]) =>
        s"${indent(depth)}FunE(${param.stringRef()} =>\n${body.prettyPrint(depth + 1)}\n${indent(depth)})"
      case Fun(param, body: DatabaseAST[?]) =>
        s"${indent(depth)}FunQ(${param.stringRef()} =>\n${body.prettyPrint(depth + 1)}\n${indent(depth)})"
      case _ => throw new Exception(s"Unimplemented pretty print FUN $fun")
  }

  extension (fun: QueryFun[?, ?]) {
    def prettyPrint(depth: Int): String = fun match
      case QueryFun(param, body: DatabaseAST[?]) =>
        s"${indent(depth)}FunR(${param.prettyPrint(0)} =>\n${body.prettyPrint(depth + 1)}\n${indent(depth)})"
  }

  extension (expr: Expr[?]) {
    def prettyPrint(depth: Int): String = expr match {
      case Select(x, name) => s"${indent(depth)}Select(${x.prettyPrint(0)}.$name)"
      case Ref() => s"${indent(depth)}${expr.asInstanceOf[Ref[?]].stringRef()}"
      case Eq(x, y) => s"${indent(depth)}Eq(\n${x.prettyPrint(depth + 1)},\n${y.prettyPrint(depth + 1)}\n${indent(depth)})"
      case Ne(x, y) => s"${indent(depth)}Ne(\n${x.prettyPrint(depth + 1)},\n${y.prettyPrint(depth + 1)}\n${indent(depth)})"
      case Gt(x, y) => s"${indent(depth)}Gt(\n${x.prettyPrint(depth + 1)},\n${y.prettyPrint(depth + 1)}\n${indent(depth)})"
      case GtDouble(x, y) => s"${indent(depth)}GtDouble(\n${x.prettyPrint(depth + 1)},\n${y.prettyPrint(depth + 1)}\n${indent(depth)})"
      case And(x, y) => s"${indent(depth)}And(\n${x.prettyPrint(depth + 1)},\n${y.prettyPrint(depth + 1)}\n${indent(depth)})"
      case Or(x, y) => s"${indent(depth)}Or(\n${x.prettyPrint(depth + 1)},\n${y.prettyPrint(depth + 1)}\n${indent(depth)})"
      case Plus(x, y) => s"${indent(depth)}Plus(\n${x.prettyPrint(depth + 1)},\n${y.prettyPrint(depth + 1)}\n${indent(depth)})"
      case Upper(x) => s"${indent(depth)}Upper(${x.prettyPrint(depth + 1)})"
      case Lower(x) => s"${indent(depth)}Lower(${x.prettyPrint(depth + 1)})"
      case Concat(x, y) =>
        s"${indent(depth)}Concat(\n${x.prettyPrint(depth + 1)},\n${y.prettyPrint(depth + 1)}\n${indent(depth)})"
      case IntLit(value) => s"${indent(depth)}IntLit($value)"
      case StringLit(value) => s"${indent(depth)}StringLit($value)"
      case DoubleLit(value) => s"${indent(depth)}DoubleLit($value)"
      case Project(inner) =>
        val a = NamedTuple.toTuple(inner.asInstanceOf[NamedTuple[Tuple, Tuple]]) // TODO: bug? See https://github.com/scala/scala3/issues/21157
        val namedTupleNames = expr.tag match
          case ResultTag.NamedTupleTag(names, types) => names.lift
          case _ => Seq()
        val children = a.toList.zipWithIndex
          .map((expr, idx) =>
            val e = expr.asInstanceOf[Expr[?]]
            val namedStr = namedTupleNames(idx).fold("")(n => s"$n=")
            s"${indent(depth+1)}$namedStr${e.prettyPrint(0)}"
          )
        s"${indent(depth)}Project(\n${children.mkString("", ",\n", "")}\n${indent(depth)})"
      case a: Aggregation[?] => a.prettyPrint(depth)
      case _ => throw new Exception(s"Unimplemented pretty print EXPR $expr")
    }
  }
  extension(agg: Aggregation[?]) {
    def prettyPrint(depth: Int): String = agg match {
      case Min(x) => s"${indent(depth)}Min(${x.prettyPrint(depth + 1).stripLeading()})"
      case Max(x) => s"${indent(depth)}Max(${x.prettyPrint(depth + 1).stripLeading()})"
      case Sum(x) => s"${indent(depth)}Sum(${x.prettyPrint(depth + 1).stripLeading()})"
      case Avg(x) => s"${indent(depth)}Avg(${x.prettyPrint(depth + 1).stripLeading()})"
      case Count(x) => s"${indent(depth)}Count(${x.prettyPrint(depth + 1).stripLeading()})"
      case AggFlatMap(from, query) =>
        s"${indent(depth)}AggFlatMap(\n${from.prettyPrint(depth + 1)},\n${query.prettyPrint(depth + 1)}\n${indent(depth)})"
      case AggProject(inner) =>
        val a = NamedTuple.toTuple(inner.asInstanceOf[NamedTuple[Tuple, Tuple]]) // TODO: bug? See https://github.com/scala/scala3/issues/21157
        val namedTupleNames = agg.tag match
          case ResultTag.NamedTupleTag(names, types) => names.lift
          case _ => Seq()
        val children = a.toList.zipWithIndex
          .map((expr, idx) =>
            val e = expr.asInstanceOf[Expr[?]]
            val namedStr = namedTupleNames(idx).fold("")(n => s"$n=")
            s"${indent(depth+1)}$namedStr${e.prettyPrint(0)}"
          )
        s"${indent(depth)}Project(\n${children.mkString("", ",\n", "")}\n${indent(depth)})"
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
      case Drop(from, offset) =>
        s"${indent(depth)}Drop(\n${from.prettyPrint(depth + 1)}, $offset\n${indent(depth)})"
      case Distinct(from) =>
        s"${indent(depth)}Distinct(\n${from.prettyPrint(depth + 1)}\n${indent(depth)})"
      case Union(thisQuery, other, dedup) =>
        s"${indent(depth)}Union(dedup=$dedup\n${thisQuery.prettyPrint(depth + 1)},\n${other.prettyPrint(depth + 1)}\n${indent(depth)})"
      case Intersect(thisQuery, other) =>
        s"${indent(depth)}Intersect(\n${thisQuery.prettyPrint(depth + 1)},\n${other.prettyPrint(depth + 1)}\n${indent(depth)})"
      case Except(thisQuery, other) =>
        s"${indent(depth)}Except(\n${thisQuery.prettyPrint(depth + 1)},\n${other.prettyPrint(depth + 1)}\n${indent(depth)})"
      case GroupBy(query, selectFn, groupingFn, havingFn) =>
        s"${indent(depth)}GroupBy(\n${query.prettyPrint(depth + 1)},\n${selectFn.prettyPrint(depth + 1)},\n${groupingFn.prettyPrint(depth + 1)},\n${havingFn.prettyPrint(depth + 1)}\n${indent(depth)})"
      case a: Aggregation[?] => a.prettyPrint(depth)
      case Recursive(from, query) =>
        s"${indent(depth)}Recursive(\n${from.prettyPrint(depth + 1)},\n${query.prettyPrint(depth + 1)}\n${indent(depth)})"
      case QueryRef() => s"${indent(depth)}QueryRef(${ast.asInstanceOf[QueryRef[?]].stringRef()})"
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
        s"${indent(depth)}BinRelationOp{${relationOp.alias}}{${relationOp.flags.mkString(",")}}(\n${indent(depth + 1)}op = '${naryRelationOp.op}'\n${childrenPrint.mkString(",\n")}$astPrint\n${indent(depth)})"
      case recursiveRelationOp: RecursiveRelationOp =>
        s"${indent(depth)}RecursiveOp{${recursiveRelationOp.alias}}{${relationOp.flags.mkString(",")}}(\n${indent(depth+1)}base=\n${recursiveRelationOp.finalQ.prettyPrintIR(depth+2, printAST)}\n${indent(depth + 1)}query=\n${recursiveRelationOp.query.prettyPrintIR(depth + 2, printAST)}"
      case recursiveIRVar: RecursiveIRVar =>
        s"${indent(depth)}RecursiveVar{${recursiveIRVar.alias}}"
      case _ => throw new Exception(s"Unimplemented pretty print RelationOp $relationOp")
    }
  }

  extension (node: QueryIRNode) {
    def prettyPrintIR(depth: Int, printAST: Boolean): String = node match {      case unaryOp: UnaryExprOp =>
        val childPrint = unaryOp.child.prettyPrintIR(depth + 1, printAST)
        val astPrint = if (printAST) s"\n${indentWithKey(depth + 1, "AST", unaryOp.ast.prettyPrint(depth + 1))}" else ""
        s"${indent(depth)}UnaryExprOp(\n${indent(depth + 1)}op='${unaryOp.op("...")}',\n$childPrint$astPrint\n${indent(depth)})"
      case binOp: BinExprOp =>
        val lhsPrint = binOp.lhs.prettyPrintIR(depth + 1, printAST)
        val rhsPrint = binOp.rhs.prettyPrintIR(depth + 1, printAST)
        val opPrint = binOp.op
        val astPrint = if (printAST) s"\n${indentWithKey(depth + 1, "AST", binOp.ast.prettyPrint(depth + 1))}" else ""
        s"${indent(depth)}BinExprOp(\n${indent(depth + 1)}op='$opPrint',\n$lhsPrint,\n$rhsPrint$astPrint\n${indent(depth)})"
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

      case relationOp: RelationOp => relationOp.prettyPrintIR(depth, printAST)

      case _ => throw new Exception(s"Unimplemented pretty print QueryIRNode $node")
    }
  }
}
