package tyql

object ASTPrinter {
  import Query.*
  import Expr.*
  
  extension (fun: Fun[?, ?]) {
    def prettyPrint(depth: Int): String = fun match
      case Fun(param, body: Expr[?]) =>
        s"${indent(depth)}FunE(${param.stringRef()} =>\n${body.prettyPrint(depth + 1)}\n${indent(depth)})"
      case Fun(param, body: DatabaseAST[?]) =>
        s"${indent(depth)}FunQ(${param.stringRef()} =>\n${body.prettyPrint(depth + 1)}\n${indent(depth)})"
  }

  extension (expr: Expr[?]) {
    def prettyPrint(depth: Int): String = expr match {
      case Select(x, name) => s"${indent(depth)}Select(${x.prettyPrint(0)}.$name)"
      case Ref() => s"${indent(depth)}${expr.asInstanceOf[Ref[?]].stringRef()}"
      case Eq(x, y) => s"${indent(depth)}Eq(\n${x.prettyPrint(depth + 1)},\n${y.prettyPrint(depth + 1)}\n${indent(depth)})"
      case And(x, y) => s"${indent(depth)}And(\n${x.prettyPrint(depth + 1)},\n${y.prettyPrint(depth + 1)}\n${indent(depth)})"
      case Or(x, y) => s"${indent(depth)}Or(\n${x.prettyPrint(depth + 1)},\n${y.prettyPrint(depth + 1)}\n${indent(depth)})"
      case Plus(x, y) => s"${indent(depth)}Plus(\n${x.prettyPrint(depth + 1)},\n${y.prettyPrint(depth + 1)}\n${indent(depth)})"
      case _ => s"${indent(depth)}Unknown Expr Node: $expr"
    }
  }

  // Extending the pretty printing functionality to `DatabaseAST` types
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
        s"${indent(depth)}Sort(ord:$ord\n${from.prettyPrint(depth + 1)},\n${body.prettyPrint(depth + 1)}\n${indent(depth)})"
      case Limit(from, limit) =>
        s"${indent(depth)}Limit(limit=$limit\n${from.prettyPrint(depth + 1)}\n${indent(depth)})"
      case _ => s"${indent(depth)}Unknown DatabaseAST Node: ${ast}"
    }
  }

  private def indent(level: Int): String = "  " * level
}
