package tyql

private type Precedence = Int
private type SnippetSpecificationPart = (String | (String, Precedence))

case class SqlSnippet(precedence: Precedence, sql: Seq[SnippetSpecificationPart])

extension (sc: StringContext) {
  def snippet(args: (String, Precedence)*): Seq[SnippetSpecificationPart] = {
    val parts = sc.parts.iterator
    val expressions = args.iterator
    val buf = Seq.newBuilder[SnippetSpecificationPart]
    while (parts.hasNext) {
      buf += parts.next()
      if (expressions.hasNext) {
        val (expr, prec) = expressions.next()
        buf += ((expr, prec))
      }
    }
    buf.result()
  }
}
